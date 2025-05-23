package com.dharmil.investsage.service;

import com.pgvector.PGvector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

@Service
public class RagDataProcessor {

    private static final Logger log = LoggerFactory.getLogger(RagDataProcessor.class);

    // --- Constant for the target table ---
    private static final String EMBEDDING_TABLE = "new_investment_embeddings"; // Use the new table exclusively

    private final OpenAiEmbeddingService embeddingService; // Still needed for query embedding
    private final JdbcTemplate jdbcTemplate;
    // Self-injection and ApplicationContext no longer needed as processing is moved to Batch

    @Autowired
    public RagDataProcessor(OpenAiEmbeddingService embeddingService, JdbcTemplate jdbcTemplate, DataSource dataSource) {
        this.embeddingService = embeddingService;
        this.jdbcTemplate = jdbcTemplate;
        try (Connection conn = dataSource.getConnection()) {
            // Still good practice to register the type on startup
            PGvector.addVectorType(conn);
            log.info("PGvector type registered with JDBC connection.");
        } catch (SQLException e) {
            // Logged as error, but allow application to start if necessary
            log.error("FATAL: Failed to register PGvector type with JDBC connection", e);
        }
        // No self-injection needed anymore
    }

    // --- Removed all embedding processing methods ---
    // processAndStoreEmbeddingsNew(), processApiChunkBatch(), processDbBatchNew()
    // processAndStoreEmbeddings(), findSimilarChunks() [deprecated ones]
    // ChunkInfo inner class

    // --- Renamed Retrieval Method (was findSimilarChunksNew) ---

    /**
     * Finds similar chunks by querying the EMBEDDING_TABLE.
     */
    public List<String> findSimilarChunks(String queryText, int limit) {
        if (!checkDatabaseConnection()) {
            log.error("Database connection check failed. Cannot perform similarity search on {}.", EMBEDDING_TABLE);
            return Collections.emptyList();
        }
        if (queryText == null || queryText.trim().isEmpty()) return Collections.emptyList();

        log.debug("Finding similar chunks in {} for query: {}", EMBEDDING_TABLE, queryText.substring(0, Math.min(queryText.length(), 100)));

        // Use single embedding getter for the query
        List<Double> queryEmbeddingDoubleList = embeddingService.getEmbedding(queryText);
        if (queryEmbeddingDoubleList.stream().allMatch(d -> d == 0.0)) {
            log.warn("Could not get embedding for query '{}'. Returning empty results.", queryText.substring(0, Math.min(queryText.length(), 100)));
            return Collections.emptyList();
        }

        // Convert List<Double> to float[] for PGvector object
        float[] queryEmbeddingFloatArray = new float[queryEmbeddingDoubleList.size()];
        for(int i=0; i<queryEmbeddingDoubleList.size(); i++) {
            // Handle potential NullPointerException if list contains nulls, though unlikely here
            if (queryEmbeddingDoubleList.get(i) == null) {
                log.error("Null value found in query embedding list at index {}. Aborting search.", i);
                return Collections.emptyList();
            }
            queryEmbeddingFloatArray[i] = queryEmbeddingDoubleList.get(i).floatValue();
        }
        PGvector queryVector = new PGvector(queryEmbeddingFloatArray);

        // Use the EMBEDDING_TABLE constant in the SQL query
        String sql = String.format(
                "SELECT chunk_text FROM %s ORDER BY embedding <=> ? LIMIT ?",
                EMBEDDING_TABLE
        );
        try {
            log.debug("Executing similarity search SQL on {}...", EMBEDDING_TABLE);
            List<String> results = jdbcTemplate.query(sql, ps -> {
                ps.setObject(1, queryVector);
                ps.setInt(2, limit);
            }, (rs, rowNum) -> rs.getString("chunk_text"));

            log.info("Found {} similar chunks in {} for query.", results.size(), EMBEDDING_TABLE);
            return results;
        } catch (Exception e) {
            log.error("Error during similarity search on {}: {}", EMBEDDING_TABLE, e.getMessage(), e);
            return Collections.emptyList();
        }
    }

    // --- Database Connection Check ---
    public final boolean checkDatabaseConnection() {
        try {
            jdbcTemplate.queryForObject("SELECT 1", Integer.class);
            log.info("Database connection check successful.");
            return true;
        } catch (Exception e) {
            log.error("Database connection check failed! Error: {}", e.getMessage());
            return false;
        }
    }
}