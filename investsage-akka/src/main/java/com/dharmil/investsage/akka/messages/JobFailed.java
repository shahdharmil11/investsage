package com.dharmil.investsage.akka.messages;

import java.util.Optional;

/**
 * Signals failure of the embedding job.
 *
 * @param reason         A description of why the job failed.
 * @param cause          Optional Throwable that caused the failure.
 * @param durationMillis Time elapsed before the job failed.
 */
public record JobFailed(String reason, Optional<Throwable> cause, long durationMillis) implements EmbeddingJobMessage {
}
