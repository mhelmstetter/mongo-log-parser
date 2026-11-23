package com.mongodb.log.parser;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.atlas.api.clients.AtlasApiClient;
import com.mongodb.atlas.api.logs.AtlasLogType;

/**
 * Downloads mongod/mongos logs for every process of an Atlas cluster via the Atlas Admin API.
 *
 * Files are downloaded in parallel into a ".partial" directory, checked by decompressing them
 * completely, and only then moved into the output directory, so a file in the output directory
 * is always complete. A file that is already there and decompresses cleanly is reused, which
 * makes an interrupted download resumable by running the same command again. Each file is
 * retried a few times (the API resets long transfers now and then); if a file still fails,
 * the whole download fails.
 */
public class AtlasLogDownloader {

    private static final Logger logger = LoggerFactory.getLogger(AtlasLogDownloader.class);

    private static final int MAX_ATTEMPTS = 3;
    private static final long[] RETRY_DELAY_SECONDS = {15, 60};
    private static final DateTimeFormatter FILE_TIME = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH_mm_ss");

    private final String publicKey;
    private final String privateKey;

    public AtlasLogDownloader(String configFile) throws IOException {
        Properties props = new Properties();
        try (FileInputStream in = new FileInputStream(configFile)) {
            props.load(in);
        }
        String publicKey = props.getProperty("apiPublicKey");
        String privateKey = props.getProperty("apiPrivateKey");
        if (publicKey == null || publicKey.isBlank() || privateKey == null || privateKey.isBlank()) {
            throw new IllegalArgumentException("Atlas config file " + configFile
                    + " must define apiPublicKey and apiPrivateKey");
        }
        this.publicKey = publicKey.trim();
        this.privateKey = privateKey.trim();
    }

    private AtlasApiClient newClient() {
        return new AtlasApiClient(publicKey, privateKey);
    }

    private record LogFile(String processId, String hostname, int port, AtlasLogType logType, Path target) {
    }

    /**
     * Download the non-audit logs (mongodb.gz / mongos.gz) for all processes in the cluster.
     *
     * @param threads number of files downloaded at the same time
     * @return paths of the downloaded .log.gz files
     */
    public List<Path> download(String projectId, String clusterName, Instant start, Instant end,
                               String outputDirectory, int threads) throws IOException {
        if (!start.isBefore(end)) {
            throw new IllegalArgumentException("Atlas start time " + start + " must be before end time " + end);
        }
        if (threads < 1) {
            throw new IllegalArgumentException("Download threads must be at least 1, got " + threads);
        }

        List<Map<String, Object>> processes = newClient().clusters().getProcessesForCluster(projectId, clusterName);
        if (processes.isEmpty()) {
            throw new IllegalStateException("No processes found for cluster '" + clusterName
                    + "' in project " + projectId);
        }

        Path outputDir = Paths.get(outputDirectory);
        Path partialDir = outputDir.resolve(".partial");
        Files.createDirectories(partialDir);

        List<LogFile> files = new ArrayList<>();
        for (Map<String, Object> process : processes) {
            String processId = (String) process.get("id");
            String typeName = (String) process.get("typeName");
            String[] hostPort = processId.split(":");
            List<AtlasLogType> logTypes = AtlasLogType.getCompatibleLogTypes(typeName, false);
            if (logTypes.isEmpty()) {
                throw new IllegalStateException("No log type known for process " + processId
                        + " with type " + typeName);
            }
            for (AtlasLogType logType : logTypes) {
                // Same naming as AtlasLogsClient: {hostname}_{start}_{end}_{LOGTYPE}.log.gz
                String name = String.format("%s_%s_%s_%s.log.gz", hostPort[0],
                        start.atZone(ZoneOffset.UTC).format(FILE_TIME), end.atZone(ZoneOffset.UTC).format(FILE_TIME),
                        logType.getCleanName().toUpperCase());
                files.add(new LogFile(processId, hostPort[0], Integer.parseInt(hostPort[1]), logType, outputDir.resolve(name)));
            }
        }

        System.out.println("☁️  Downloading " + files.size() + " Atlas log files for cluster " + clusterName + " ("
                + processes.size() + " processes, " + threads + " at a time) " + start + " → " + end);

        AtomicInteger done = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Future<Path>> futures = new ArrayList<>();
        for (LogFile f : files) {
            futures.add(pool.submit(() -> fetch(projectId, clusterName, start, end, f, partialDir, done, files.size())));
        }
        pool.shutdown();
        List<Path> downloaded = new ArrayList<>();
        try {
            for (Future<Path> future : futures) {
                downloaded.add(future.get());
            }
        } catch (ExecutionException e) {
            pool.shutdownNow();
            Throwable cause = e.getCause();
            throw new IOException("Atlas log download failed: " + cause.getMessage(), cause);
        } catch (InterruptedException e) {
            pool.shutdownNow();
            Thread.currentThread().interrupt();
            throw new IOException("Atlas log download interrupted", e);
        }

        System.out.println("✅ " + downloaded.size() + " Atlas log file(s) complete in " + outputDirectory);
        return downloaded;
    }

    private Path fetch(String projectId, String clusterName, Instant start, Instant end, LogFile f,
                       Path partialDir, AtomicInteger done, int total) throws Exception {
        if (Files.exists(f.target())) {
            try {
                long bytes = verifyGzip(f.target());
                System.out.printf("♻️  [%d/%d] %s already complete (%,d bytes uncompressed), reusing%n",
                        done.incrementAndGet(), total, f.target().getFileName(), bytes);
                return f.target();
            } catch (IOException e) {
                System.out.printf("⚠️  %s exists but is not a complete gzip file (%s); downloading it again%n",
                        f.target().getFileName(), e.getMessage());
            }
        }
        AtlasApiClient client = newClient();
        Exception last = null;
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            try {
                Path partial = client.logs().downloadCompressedLogFileForHost(projectId, clusterName, f.hostname(),
                        f.port(), f.logType(), start, end, partialDir.toString());
                if (!partial.getFileName().equals(f.target().getFileName())) {
                    throw new IllegalStateException("Atlas client wrote " + partial.getFileName()
                            + ", expected " + f.target().getFileName());
                }
                long bytes = verifyGzip(partial);
                Files.move(partial, f.target(), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
                System.out.printf("📥 [%d/%d] %s (%,d bytes compressed, %,d uncompressed)%n", done.incrementAndGet(),
                        total, f.target().getFileName(), Files.size(f.target()), bytes);
                return f.target();
            } catch (Exception e) {
                last = e;
                logger.warn("Download of {} for {} failed (attempt {}/{}): {}", f.logType().getFileName(),
                        f.processId(), attempt, MAX_ATTEMPTS, e.getMessage());
                if (attempt < MAX_ATTEMPTS) {
                    System.out.printf("🔁 %s attempt %d/%d failed (%s); retrying in %ds%n", f.target().getFileName(),
                            attempt, MAX_ATTEMPTS, e.getMessage(), RETRY_DELAY_SECONDS[attempt - 1]);
                    Thread.sleep(RETRY_DELAY_SECONDS[attempt - 1] * 1000);
                }
            }
        }
        throw new IOException(f.logType().getFileName() + " for " + f.processId() + " failed after "
                + MAX_ATTEMPTS + " attempts: " + last.getMessage(), last);
    }

    /** Decompress the whole file; throws if it is truncated or corrupt. Returns the uncompressed size. */
    static long verifyGzip(Path file) throws IOException {
        long total = 0;
        byte[] buf = new byte[1 << 16];
        try (InputStream in = new GZIPInputStream(Files.newInputStream(file), 1 << 16)) {
            int n;
            while ((n = in.read(buf)) > 0) {
                total += n;
            }
        }
        if (total == 0) {
            throw new IOException("decompresses to 0 bytes");
        }
        return total;
    }

    /**
     * Parse an Atlas time argument: ISO instant (2024-01-15T10:00:00Z), local date-time
     * (2024-01-15T10:00:00, interpreted as UTC) or date (2024-01-15, start of day UTC).
     */
    public static Instant parseTime(String value) {
        try {
            return Instant.parse(value);
        } catch (DateTimeParseException e) {
            // not an instant, try the other accepted formats
        }
        try {
            return LocalDateTime.parse(value).toInstant(ZoneOffset.UTC);
        } catch (DateTimeParseException e) {
            // not a local date-time, try a plain date
        }
        try {
            return LocalDate.parse(value).atStartOfDay(ZoneOffset.UTC).toInstant();
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("Invalid Atlas time '" + value
                    + "'; expected 2024-01-15T10:00:00Z, 2024-01-15T10:00:00 or 2024-01-15");
        }
    }
}
