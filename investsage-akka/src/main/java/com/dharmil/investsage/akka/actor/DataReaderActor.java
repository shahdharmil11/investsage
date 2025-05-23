package com.dharmil.investsage.akka.actor;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.dharmil.investsage.akka.di.SpringAkkaExtension;
import com.dharmil.investsage.akka.messages.RangeReadComplete;
import com.dharmil.investsage.akka.messages.RawDataBatch;
import com.dharmil.investsage.akka.messages.ReadDataRange;
import com.dharmil.investsage.model.RawDataRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

@Component("dataReaderActor")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class DataReaderActor extends AbstractActor {

    private static final Logger log = LoggerFactory.getLogger(DataReaderActor.class);

    // --- Dependencies ---
    @Autowired
    private JdbcTemplate jdbcTemplate;

    private final ReaderConfiguration readerConfig;
    private final ActorRef chunkerActorRef;
    private final ActorRef coordinatorRef;
    private final int readerId;

    // --- Actor State ---
    private int startId = -1;
    private int endId = -1;
    private int lastIdRead = -1;
    private boolean rangeProcessingStarted = false;
    private boolean rangeCompleted = false;
    private long pagesProcessed = 0; // Counter for pages
    private long recordsSentThisReader = 0; // Counter for records sent

    // --- Configuration Class ---
    public record ReaderConfiguration(int dbPageSize) {}

    // --- Props Factory ---
    public static Props props(ActorSystem system, int readerId, ReaderConfiguration readerConfig, ActorRef chunkerActorRef, ActorRef coordinatorRef) {
        return SpringAkkaExtension.provider.get(system).props("dataReaderActor", readerId, readerConfig, chunkerActorRef, coordinatorRef);
    }

    // --- Constructor ---
    @Autowired(required = false)
    public DataReaderActor(int readerId, ReaderConfiguration readerConfig, ActorRef chunkerActorRef, ActorRef coordinatorRef) {
        this.readerId = readerId;
        this.readerConfig = readerConfig;
        this.chunkerActorRef = chunkerActorRef;
        this.coordinatorRef = coordinatorRef;
        // Log moved to preStart for better timing relative to injection
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        log.info("Reader {} starting...", readerId); // Log start
        String dispatcherId = getContext().getProps().dispatcher();
        log.info("Reader {} preStart: Configured dispatcher ID: {}", readerId, dispatcherId);

        if (jdbcTemplate == null) {
            log.error("JdbcTemplate not injected correctly into DataReaderActor {}!", readerId);
            // Report failure before throwing? Might be complex this early.
            throw new IllegalStateException("Required dependency JdbcTemplate not injected by Spring.");
        }
        if (readerConfig == null || chunkerActorRef == null || coordinatorRef == null) {
            log.error("Required constructor arguments missing for DataReaderActor {}!", readerId);
            throw new IllegalStateException("Required constructor arguments missing.");
        }
        log.info("Reader {} preStart finished successfully. Ready for messages.", readerId);
    }

    // --- Receive Method ---
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ReadDataRange.class, this::canStartProcessing, msg -> handleReadDataRange(msg))
                .matchAny(msg -> handleUnknownMessage(msg))
                .build();
    }

    // --- Predicates ---
    private boolean canStartProcessing(ReadDataRange msg) {
        // Log if message is ignored due to state
        if (rangeProcessingStarted) {
            log.warn("Reader {} received ReadDataRange but already started processing. Ignoring message: {}", readerId, msg);
        }
        return !rangeProcessingStarted;
    }

    // --- Message Handlers ---
    private void handleReadDataRange(ReadDataRange msg) {
        log.info("Reader {} received ReadDataRange message: {}", readerId, msg);

        this.rangeProcessingStarted = true;
        this.startId = msg.startId();
        this.endId = msg.endId();
        // Initialize correctly before first read. IDs are > lastIdRead and < endId.
        this.lastIdRead = this.startId - 1;
        this.pagesProcessed = 0; // Reset page count
        this.recordsSentThisReader = 0; // Reset record count

        log.info("Reader {} INITIALIZING processing for range [{}, {}). Page size: {}",
                readerId, this.startId, this.endId, readerConfig.dbPageSize());

        // Initiate the first read
        readAndProcessNextPage();
    }

    private void handleUnknownMessage(Object msg) {
        log.warn("Reader {} received unknown message type: {} from {}", readerId, msg.getClass().getName(), getSender());
    }

    // --- Helper Methods ---

    private void readAndProcessNextPage() {
        if (rangeCompleted) {
            log.debug("Reader {} range already marked completed, skipping read.", readerId);
            return;
        }
        if (jdbcTemplate == null) {
            log.error("JdbcTemplate is null in readAndProcessNextPage for reader {}. Stopping actor.", readerId);
            // Optionally report failure to coordinator before stopping
            // coordinatorRef.tell(new JobFailed(...), getSelf());
            getContext().stop(getSelf()); // Stop self if critical dependency missing
            return;
        }

        pagesProcessed++;
        log.info("Reader {} starting read for page {}.", readerId, pagesProcessed);

        try {
            String sql = "SELECT id, raw_text FROM raw_investment_data WHERE id > ? AND id < ? ORDER BY id LIMIT ?";

            // --- Log query parameters ---
            log.info("Reader {} preparing query for page {}: SQL='{}', params=[lastIdRead={}, endId={}, limit={}]",
                    readerId, pagesProcessed, sql, lastIdRead, endId, readerConfig.dbPageSize());

            long queryStartTime = System.nanoTime();
            List<RawDataRecord> records = jdbcTemplate.query(
                    sql,
                    ps -> {
                        ps.setInt(1, lastIdRead);
                        ps.setInt(2, endId);
                        ps.setInt(3, readerConfig.dbPageSize());
                    },
                    new BeanPropertyRowMapper<>(RawDataRecord.class)
            );
            long queryEndTime = System.nanoTime();
            int recordsFetched = records.size();

            // --- Log query results ---
            log.info("Reader {} query for page {} completed in {} ms, fetched {} records.",
                    readerId, pagesProcessed, (queryEndTime - queryStartTime) / 1_000_000, recordsFetched);

            if (recordsFetched > 0) {
                // --- Log before sending ---
                log.debug("Reader {} sending RawDataBatch with {} records to {}", readerId, recordsFetched, chunkerActorRef);
                RawDataBatch batchMsg = new RawDataBatch(records);
                chunkerActorRef.tell(batchMsg, getSelf());
                recordsSentThisReader += recordsFetched; // Increment counter

                int idOfLastRecordInPage = records.get(recordsFetched - 1).getId();
                // --- Log ID update ---
                log.info("Reader {} page {} finished. Last record ID was: {}. Updating lastIdRead from {}.",
                        readerId, pagesProcessed, idOfLastRecordInPage, this.lastIdRead);
                this.lastIdRead = idOfLastRecordInPage;

                // --- Log loop decision ---
                if (recordsFetched == readerConfig.dbPageSize()) {
                    log.info("Reader {} fetched a full page ({}), requesting next page.", readerId, recordsFetched);
                    // Use scheduler for slight delay to avoid potential stack overflow on deep recursion
                    // Though direct call is often fine if DB query provides sufficient backpressure.
                    // Sticking with direct call for now based on previous code.
                    readAndProcessNextPage();
                } else {
                    log.info("Reader {} fetched a partial page ({} records < page size {}), indicating end of range.",
                            readerId, recordsFetched, readerConfig.dbPageSize());
                    completeRangeProcessing();
                }
            } else {
                // --- Log loop decision (0 records) ---
                log.info("Reader {} fetched 0 records on page {}. Assuming end of range [{}, {}).",
                        readerId, pagesProcessed, startId, endId);
                completeRangeProcessing();
            }

        } catch (DataAccessException e) {
            log.error("Reader {} caught DataAccessException during DB query for page {}. Last ID read: {}. Error: {}",
                    readerId, pagesProcessed, lastIdRead, e.getMessage(), e);
            // Let supervision strategy handle actor stop by throwing exception
            throw new RuntimeException("DataReader " + readerId + " failed DB query", e);
        } catch (Exception e) {
            log.error("Reader {} caught UNEXPECTED Exception during DB processing for page {}. Last ID read: {}. Error: {}",
                    readerId, pagesProcessed, lastIdRead, e.getMessage(), e);
            // Let supervision strategy handle actor stop by throwing exception
            throw new RuntimeException("DataReader " + readerId + " failed unexpectedly", e);
        }
    }

    private void completeRangeProcessing() {
        if (!rangeCompleted) {
            this.rangeCompleted = true;
            // --- Log final counts for this reader ---
            log.info("Reader {} COMPLETED processing for range [{}, {}). Processed {} pages, sent total {} records.",
                    readerId, startId, endId, pagesProcessed, recordsSentThisReader);
            coordinatorRef.tell(new RangeReadComplete(readerId), getSelf());
        } else {
            log.debug("Reader {} already marked as completed.", readerId);
        }
    }
}