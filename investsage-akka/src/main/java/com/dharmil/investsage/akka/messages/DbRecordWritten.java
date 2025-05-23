package com.dharmil.investsage.akka.messages;

/**
 * Sent by a DbWriterActor to the JobCoordinatorActor after successfully writing a batch to the database.
 *
 * @param count The number of records written in the batch.
 */
public record DbRecordWritten(int count) implements EmbeddingJobMessage {
}
