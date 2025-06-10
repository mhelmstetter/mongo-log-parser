package com.mongodb.log.parser;

import java.util.List;

public class ParsedChunk {

	private final int index;
    private final List<String> parsedLines;

    public ParsedChunk(int index, List<String> parsedLines) {
        this.index = index;
        this.parsedLines = parsedLines;
    }

    public int getIndex() {
        return index;
    }

    public List<String> getParsedLines() {
        return parsedLines;
    }
}
