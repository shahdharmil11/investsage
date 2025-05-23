package com.dharmil.investsage.akka.messages;

/**
 * Signals successful completion of the entire embedding job.
 * @param durationMillis Total time taken for the job.
 */
public record JobCompleted(long durationMillis) implements EmbeddingJobMessage {}

