package com.dharmil.investsage.batch;


// Represents the final record to be written to new_investment_embeddings
public class EmbeddingRecord {
    private int rawDataId;
    private int chunkIndex;
    private String chunkText;
    private float[] embedding; // Use float[] for PGvector compatibility in writer

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
