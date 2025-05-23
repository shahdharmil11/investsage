package com.dharmil.investsage.util;

import java.util.ArrayList;
import java.util.List;

public class TextChunker {
    // Simple fixed-size chunking with overlap
    public static List<String> chunkText(String text, int chunkSize, int overlap) {
        List<String> chunks = new ArrayList<>();
        if (text == null || text.trim().isEmpty()) {
            return chunks;
        }
        text = text.trim(); // Ensure no leading/trailing whitespace
        if (text.length() <= chunkSize) {
            chunks.add(text);
            return chunks;
        }

        int start = 0;
        while (start < text.length()) {
            int end = Math.min(start + chunkSize, text.length());
            chunks.add(text.substring(start, end));
            int step = chunkSize - overlap;
            if (step <= 0) { // Prevent infinite loop if overlap >= chunk size
                start += 1; // Move forward by at least 1
                System.err.println("Warning: Chunk overlap is >= chunk size. Advancing by 1.");
            } else {
                start += step;
            }
        }
        return chunks;
    }
}
