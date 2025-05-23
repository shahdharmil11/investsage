package com.dharmil.investsage.batch;


// Represents a single chunk with metadata before embedding
public class ChunkInfo {
    private final int rawDataId;
    private final int chunkIndex;
    private final String text;

    public ChunkInfo(int rawDataId, int chunkIndex, String text) {
        this.rawDataId = rawDataId;
        this.chunkIndex = chunkIndex;
        this.text = text;
    }

    public int getRawDataId() { return rawDataId; }
    public int getChunkIndex() { return chunkIndex; }
    public String getText() { return text; }
}
