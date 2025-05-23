package com.dharmil.investsage.akka.messages;

/**
 * Sent by a DataReaderActor to the JobCoordinatorActor when it has finished reading its assigned range.
 *
 * @param readerId An identifier for the reader (e.g., its partition number) for tracking.
 */
public record RangeReadComplete(int readerId) implements EmbeddingJobMessage {
}
