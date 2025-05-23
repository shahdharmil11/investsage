package com.dharmil.investsage.akka.messages;

/**
 * Tells a DataReaderActor which range of primary keys to read.
 *
 * @param startId The starting ID (inclusive).
 * @param endId   The ending ID (exclusive).
 */
public record ReadDataRange(int startId, int endId) implements EmbeddingJobMessage {
}
