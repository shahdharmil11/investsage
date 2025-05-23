package com.dharmil.investsage.batch;

import com.dharmil.investsage.service.OpenAiEmbeddingService;
import com.pgvector.PGvector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.lang.NonNull;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class EmbeddingItemWriter implements ItemWriter<List<ChunkInfo>> { // Writes List<ChunkInfo> from processor

    private static final Logger log = LoggerFactory.getLogger(EmbeddingItemWriter.class);

    private final OpenAiEmbeddingService embeddingService;
    private final JdbcTemplate jdbcTemplate;

    // Constants matching RagDataProcessor for consistency (can be injected/configured)
    private static final String NEW_EMBEDDING_TABLE = "new_investment_embeddings";
    private static final int API_BATCH_SIZE = 500; // OpenAI API batch size

    public EmbeddingItemWriter(OpenAiEmbeddingService embeddingService, JdbcTemplate jdbcTemplate) {
        this.embeddingService = embeddingService;
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void write(@NonNull Chunk<? extends List<ChunkInfo>> chunk) throws Exception {
        // 1. Flatten the chunk of lists into a single list of ChunkInfo
        List<ChunkInfo> allChunkInfos = new ArrayList<>();
        for (List<ChunkInfo> list : chunk.getItems()) {
            if (list != null) { // Processor might return null
                allChunkInfos.addAll(list);
            }
        }

        if (allChunkInfos.isEmpty()) {
            log.debug("Writer received empty chunk, nothing to write.");
            return;
        }

        log.debug("Writer received {} total chunks to process.", allChunkInfos.size());

        // 2. Process in batches suitable for the API
        List<EmbeddingRecord> recordsToWrite = new ArrayList<>();
        for (int i = 0; i < allChunkInfos.size(); i += API_BATCH_SIZE) {
            int end = Math.min(i + API_BATCH_SIZE, allChunkInfos.size());
            List<ChunkInfo> apiBatch = allChunkInfos.subList(i, end);

            if (apiBatch.isEmpty()) continue;

            log.debug("Processing API batch of {} chunks (from index {} to {})...", apiBatch.size(), i, end-1);

            // 3. Call Batch Embedding Service
            List<String> textsToEmbed = apiBatch.stream().map(ChunkInfo::getText).collect(Collectors.toList());
            List<List<Double>> embeddings = embeddingService.getEmbeddings(textsToEmbed); // Assuming this handles errors/empty results internally

            if (embeddings.size() != apiBatch.size()) {
                log.error("API embedding count mismatch! Expected {}, got {}. Skipping this API batch.", apiBatch.size(), embeddings.size());
                // Consider more robust error handling - maybe retry?
                continue; // Skip writing this failed API batch
            }

            // 4. Prepare EmbeddingRecord objects for DB Write
            for (int j = 0; j < apiBatch.size(); j++) {
                ChunkInfo info = apiBatch.get(j);
                List<Double> embedding = embeddings.get(j);

                if (embedding.stream().allMatch(d -> d == 0.0)) {
                    log.warn("Skipping zero vector from API for chunk from raw_id: {}, index: {}", info.getRawDataId(), info.getChunkIndex());
                    continue; // Don't write failed embeddings
                }

                // Convert to float[] for PGvector
                float[] embeddingFloatArray = new float[embedding.size()];
                for (int k = 0; k < embedding.size(); k++) {
                    embeddingFloatArray[k] = embedding.get(k).floatValue();
                }
                // PGvector pgVector = new PGvector(embeddingFloatArray); // PGvector created during insertion

                recordsToWrite.add(new EmbeddingRecord(
                        info.getRawDataId(),
                        info.getChunkIndex(),
                        info.getText(),
                        embeddingFloatArray // Pass float array
                ));
            }
        } // End of API batch loop

        // 5. Write the prepared EmbeddingRecords to the Database using JdbcTemplate batchUpdate
        if (!recordsToWrite.isEmpty()) {
            writeBatchToDb(recordsToWrite);
        } else {
            log.debug("No valid embedding records generated from the chunk to write.");
        }
    }


    // Helper method for DB batch writing
    private void writeBatchToDb(List<EmbeddingRecord> records) {
        String sql = String.format(
                "INSERT INTO %s (raw_data_id, chunk_index, chunk_text, embedding) VALUES (?, ?, ?, ?)",
                NEW_EMBEDDING_TABLE
        );

        log.debug("Writing batch of {} embedding records to DB.", records.size());

        try {
            jdbcTemplate.batchUpdate(sql, records, records.size(), // Use batch size = list size here
                    (PreparedStatement ps, EmbeddingRecord record) -> {
                        ps.setInt(1, record.getRawDataId());
                        ps.setInt(2, record.getChunkIndex());
                        ps.setString(3, record.getChunkText());
                        ps.setObject(4, new PGvector(record.getEmbedding())); // Create PGvector here
                    });
            log.info("Successfully wrote batch of {} records to {}.", records.size(), NEW_EMBEDDING_TABLE);
        } catch (Exception e) {
            // Log error, Spring Batch will handle retry/skip logic based on job config
            log.error("Error writing batch of {} records to DB: {}", records.size(), e.getMessage(), e);
            // Re-throw for Spring Batch to handle
            throw new RuntimeException("Error writing batch to database", e);
        }
    }
}