package com.dharmil.investsage.akka.messages;

import akka.actor.ActorRef;

/**
 * Message to initiate the embedding generation job.
 *
 * @param replyTo The actor to notify upon job completion or failure.
 */
public record StartEmbeddingJob(ActorRef replyTo) implements EmbeddingJobMessage {
}
