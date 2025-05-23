package com.dharmil.investsage.akka.messages;

import com.dharmil.investsage.model.ChunkInfo;

/**
 * Contains a single text chunk ready for embedding.
 *
 * @param chunkInfo Information about the chunk (original ID, index, text).
 */
public record ChunkToEmbed(ChunkInfo chunkInfo) implements EmbeddingJobMessage {
}
