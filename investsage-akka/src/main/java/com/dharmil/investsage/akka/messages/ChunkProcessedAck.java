package com.dharmil.investsage.akka.messages;

/**
 * Sent from EmbeddingActor back to ChunkerActor to signal
 * that one chunk has finished processing (success or failure),
 * allowing the Chunker to potentially send more work.
 */
public record ChunkProcessedAck() implements EmbeddingJobMessage {
    // No data needed, just the signal
    private static final long serialVersionUID = 1L; // Add serialVersionUID
}