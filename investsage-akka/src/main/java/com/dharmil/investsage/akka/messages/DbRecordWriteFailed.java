package com.dharmil.investsage.akka.messages;

import java.util.Optional;

/**
 * Signals that a DbWriterActor failed to write a batch to the database.
 * @param reason A description of the failure.
 * @param cause Optional underlying exception.
 */
public record DbRecordWriteFailed(String reason, Optional<Throwable> cause) implements EmbeddingJobMessage {}
