package com.dharmil.investsage.akka.messages;

import com.dharmil.investsage.model.EmbeddingRecord;

/**
 * Contains the result of an embedding operation (including the embedding vector).
 * Sent from EmbeddingActor to DbWriterActor (via router).
 *
 * @param embeddingRecord The complete record ready for storage (potentially excluding the PGvector object itself).
 */
public record EmbeddingResult(EmbeddingRecord embeddingRecord) implements EmbeddingJobMessage {
}
