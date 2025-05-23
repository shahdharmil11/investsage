package com.dharmil.investsage.akka.messages;

import java.io.Serializable;

/**
 * Marker interface for all messages related to the Embedding Generation Job.
 * Implementing Serializable is good practice for Akka messages.
 */
public interface EmbeddingJobMessage extends Serializable {
    long serialVersionUID = 1L; // Recommended for Serializable classes
}

