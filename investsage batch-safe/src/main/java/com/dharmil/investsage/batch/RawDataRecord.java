package com.dharmil.investsage.batch;

// Represents a record read from the raw_investment_data table
public class RawDataRecord {
    private int id;
    private String rawText;

    // Default constructor (needed by some Spring Batch components)
    public RawDataRecord() {}

    // Getters and Setters
    public int getId() { return id; }
    public void setId(int id) { this.id = id; }
    public String getRawText() { return rawText; }
    public void setRawText(String rawText) { this.rawText = rawText; }
}


