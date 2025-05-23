package com.dharmil.investsage.akka.actor;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.dharmil.investsage.akka.di.SpringAkkaExtension;
import com.dharmil.investsage.akka.messages.DbRecordWriteFailed;
import com.dharmil.investsage.akka.messages.DbRecordWritten;
import com.dharmil.investsage.akka.messages.EmbeddingResult;
import com.dharmil.investsage.model.EmbeddingRecord;
import com.pgvector.PGvector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.Types;
import java.util.Optional;

@Component("dbWriterActor")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class DbWriterActor extends AbstractActor {

    private static final Logger log = LoggerFactory.getLogger(DbWriterActor.class);

    // --- Dependencies ---
    @Autowired
    private JdbcTemplate jdbcTemplate;
    private final ActorRef coordinatorRef;
    // Removed WriterConfiguration

    // --- Actor State ---
    // Removed buffer and flushTimer

    // --- Configuration Class ---
    // Removed WriterConfiguration record

    // --- Props Factory ---
    // Updated props factory to remove WriterConfiguration
    public static Props props(ActorSystem system, ActorRef coordinatorRef) {
        return SpringAkkaExtension.provider.get(system).props("dbWriterActor", coordinatorRef);
    }

    // --- Constructor ---
    // Updated constructor to remove WriterConfiguration
    @Autowired(required = false)
    public DbWriterActor(ActorRef coordinatorRef) {
        this.coordinatorRef = coordinatorRef;
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        // Updated log message
        log.info("DbWriterActor instance started via Spring.");
        if (jdbcTemplate == null) {
            log.error("JdbcTemplate not injected correctly into DbWriterActor!");
            throw new IllegalStateException("Required dependency JdbcTemplate not injected by Spring.");
        }
        // Updated check
        if (coordinatorRef == null) {
            log.error("Required constructor argument coordinatorRef missing for DbWriterActor!");
            throw new IllegalStateException("Required constructor argument coordinatorRef missing.");
        }
    }

    // --- Lifecycle Hooks ---
    @Override
    public void postStop() throws Exception {
        // Updated log message, removed buffer flushing logic
        log.info("DbWriterActor stopping.");
        super.postStop();
    }

    // --- Receive Method ---
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(EmbeddingResult.class, this::handleEmbeddingResult) // Simplified match
                // Removed matches for InternalPerformDbWrite and FlushWriteBufferTimeout
                .matchAny(this::handleUnknownMessage)
                .build();
    }

    // --- Message Handlers ---
    // Simplified handler for EmbeddingResult
    private void handleEmbeddingResult(EmbeddingResult msg) {
        log.trace("Received EmbeddingResult for rawId {}, chunkIndex {}",
                msg.embeddingRecord().getRawDataId(), msg.embeddingRecord().getChunkIndex());

        // Directly attempt to write the single record
        performSingleDbWrite(msg.embeddingRecord());
    }

    // Removed handleFlushWriteBufferTimeout and handleInternalPerformDbWrite

    private void handleUnknownMessage(Object msg) {
        log.warn("Received unknown message: {} from {}", msg.getClass().getName(), getSender());
    }


    // --- Helper Methods ---

    /**
     * Performs the actual database insert operation for a single record using JdbcTemplate.
     * Reports success or failure back to the coordinator using DbRecordWritten/DbRecordWriteFailed.
     *
     * @param record The EmbeddingRecord to write.
     */
    private void performSingleDbWrite(EmbeddingRecord record) {
        if (jdbcTemplate == null) {
            log.error("JdbcTemplate is null in performSingleDbWrite. Actor may not have started correctly.");
            // Optionally, report failure and stop
            String reason = "JdbcTemplate is null, cannot perform write.";
            coordinatorRef.tell(new DbRecordWriteFailed(reason, Optional.empty()), getSelf()); // Using renamed message
            getContext().stop(getSelf());
            return;
        }

        String sql = "INSERT INTO new_investment_embeddings (raw_data_id, chunk_index, chunk_text, embedding) VALUES (?, ?, ?, ?)";
        long startTime = System.currentTimeMillis();

        log.debug("Attempting database write for rawId {}, chunkIndex {}.", record.getRawDataId(), record.getChunkIndex());

        try {
            int rowsAffected = jdbcTemplate.update(sql, ps -> {
                ps.setInt(1, record.getRawDataId());
                ps.setInt(2, record.getChunkIndex());
                ps.setString(3, record.getChunkText());
                if (record.getEmbedding() != null) {
                    ps.setObject(4, new PGvector(record.getEmbedding()));
                } else {
                    log.warn("Embedding was null for rawId {}, chunkIndex {}. Setting DB embedding to NULL.",
                            record.getRawDataId(), record.getChunkIndex());
                    ps.setNull(4, Types.OTHER);
                }
            });

            long duration = System.currentTimeMillis() - startTime;

            if (rowsAffected == 1) {
                log.debug("Successfully wrote record for rawId {}, chunkIndex {}. Duration: {} ms.",
                        record.getRawDataId(), record.getChunkIndex(), duration);
                // Report success back to coordinator using the renamed message
                coordinatorRef.tell(new DbRecordWritten(1), getSelf());
            } else {
                // This case (0 rows affected on INSERT without error) is unusual but possible
                String reason = String.format("Database write for rawId %d, chunkIndex %d reported %d rows affected (expected 1).",
                        record.getRawDataId(), record.getChunkIndex(), rowsAffected);
                log.error(reason);
                // Report failure back to coordinator using the renamed message
                coordinatorRef.tell(new DbRecordWriteFailed(reason, Optional.empty()), getSelf());
            }

        } catch (DataAccessException e) {
            String reason = String.format("Database write FAILED for rawId %d, chunkIndex %d due to DataAccessException",
                    record.getRawDataId(), record.getChunkIndex());
            log.error(reason, e);
            // Report failure back to coordinator using the renamed message
            coordinatorRef.tell(new DbRecordWriteFailed(reason, Optional.of(e)), getSelf());
        } catch (Exception e) {
            String reason = String.format("Unexpected error during database write for rawId %d, chunkIndex %d",
                    record.getRawDataId(), record.getChunkIndex());
            log.error(reason, e);
            // Report failure back to coordinator using the renamed message
            coordinatorRef.tell(new DbRecordWriteFailed(reason, Optional.of(e)), getSelf());
        }
    }

    // Removed performDbWrite (batch version)
    // Removed scheduleFlushTimer
    // Removed cancelFlushTimer
}