package com.dharmil.investsage.akka.messages;

import com.dharmil.investsage.model.RawDataRecord;

import java.util.List;

/**
 * Contains a batch of raw data records read from the database.
 *
 * @param records List of records read in this batch.
 */
public record RawDataBatch(List<RawDataRecord> records) implements EmbeddingJobMessage {
}
