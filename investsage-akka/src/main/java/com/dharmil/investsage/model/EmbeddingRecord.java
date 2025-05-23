package com.dharmil.investsage.model; // Updated package

// Represents the final record content before writing to new_investment_embeddings
// PGvector object will be created during the write phase.
public class EmbeddingRecord {
    private int rawDataId;
    private int chunkIndex;
    private String chunkText;
    private float[] embedding; // Use float[]

    // Default constructor
    public EmbeddingRecord() {}

    public EmbeddingRecord(int rawDataId, int chunkIndex, String chunkText, float[] embedding) {
        this.rawDataId = rawDataId;
        this.chunkIndex = chunkIndex;
        this.chunkText = chunkText;
        this.embedding = embedding;
    }

    // Getters and Setters
    public int getRawDataId() { return rawDataId; }
    public void setRawDataId(int rawDataId) { this.rawDataId = rawDataId; }
    public int getChunkIndex() { return chunkIndex; }
    public void setChunkIndex(int chunkIndex) { this.chunkIndex = chunkIndex; }
    public String getChunkText() { return chunkText; }
    public void setChunkText(String chunkText) { this.chunkText = chunkText; }
    public float[] getEmbedding() { return embedding; }
    public void setEmbedding(float[] embedding) { this.embedding = embedding; }
}