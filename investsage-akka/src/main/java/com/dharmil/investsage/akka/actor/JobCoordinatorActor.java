package com.dharmil.investsage.akka.actor;

import akka.actor.*;
import akka.dispatch.Dispatchers;
import akka.japi.pf.DeciderBuilder;
import akka.routing.RoundRobinPool;
import com.dharmil.investsage.akka.actor.ChunkerActor.ChunkerConfiguration;
import com.dharmil.investsage.akka.actor.DataReaderActor.ReaderConfiguration;
import com.dharmil.investsage.akka.di.SpringAkkaExtension;
import com.dharmil.investsage.akka.messages.*;
import com.dharmil.investsage.service.OpenAiEmbeddingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


@Component("jobCoordinatorActor")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class JobCoordinatorActor extends AbstractActor {

    private static final Logger log = LoggerFactory.getLogger(JobCoordinatorActor.class);

    // --- Dependencies ---
    @Autowired private JdbcTemplate jdbcTemplate;
    // embeddingService injection kept for future use? Not directly used here.
    @Autowired private OpenAiEmbeddingService embeddingService;

    // --- Configuration ---
    private final JobConfiguration jobConfig;
    // --- ADDED: Toggle for partial processing ---
    private final boolean processPartialData = true; // Set to false to process everything
    private final double partialDataFraction = 0.3; // Process 30%

    // --- Actor State ---
    // JobStatus enum including WAITING_BEFORE_CHUNKER_SHUTDOWN
    private enum JobStatus {
        IDLE, READING, STOPPING_READERS, WAITING_BEFORE_CHUNKER_SHUTDOWN,
        STOPPING_CHUNKERS, STOPPING_EMBEDDERS, WAITING_BEFORE_WRITER_SHUTDOWN,
        STOPPING_WRITERS, STOPPING_FAILED, COMPLETED, FAILED
    }
    private JobStatus status = JobStatus.IDLE;
    private long jobStartTime;
    private ActorRef originalRequester = null;
    private long totalRecordsToProcess = -1; // Represents the *effective* count for this run
    private final AtomicLong recordsWritten = new AtomicLong(0);
    private final AtomicInteger activeReaders = new AtomicInteger(0);
    private final Set<ActorRef> watchedChildren = new HashSet<>();
    private ActorRef chunkerActor = null;
    private ActorRef embeddingRouter = null;
    private ActorRef dbWriterRouter = null;
    private Throwable firstFailure = null;
    // Internal messages for delayed shutdown
    private record InitiateChunkerShutdown() implements EmbeddingJobMessage {}
    private record InitiateDbWriterShutdown() implements EmbeddingJobMessage {}


    // JobConfiguration record - Uses original (pre-throttling) ChunkerConfig args
    public record JobConfiguration(
            int numberOfReaders, int embeddingRouterPoolSize, int dbWriterRouterPoolSize,
            int dbPageSize, int chunkerTargetSize, int chunkerMinSize
            // Removed DB write args assuming sequential writes
    ) {}

    public static Props props(ActorSystem system, JobConfiguration jobConfig) {
        return SpringAkkaExtension.provider.get(system).props("jobCoordinatorActor", jobConfig);
    }

    @Autowired(required = false)
    public JobCoordinatorActor(JobConfiguration jobConfig) { this.jobConfig = jobConfig; }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        log.info("JobCoordinatorActor instance started. Config: {}, PartialProcessingEnabled: {}", jobConfig, processPartialData);
        if (jdbcTemplate == null) { throw new IllegalStateException("JdbcTemplate not injected"); }
        if (jobConfig == null) { throw new IllegalStateException("JobConfiguration is null"); }
    }


    // --- Supervision Strategy ---
    private static final SupervisorStrategy strategy = new OneForOneStrategy(
            5, Duration.ofMinutes(1),
            DeciderBuilder
                    .match(RuntimeException.class, e -> {
                        log.error("Child actor failed with RuntimeException. Stopping it.", e);
                        return SupervisorStrategy.stop();
                    })
                    .matchAny(o -> {
                        log.error("Child actor failed with unhandled Throwable type [{}]. Escalating.", o.getClass().getName(), o);
                        return SupervisorStrategy.escalate();
                    })
                    .build());
    @Override public SupervisorStrategy supervisorStrategy() { return strategy; }

    // --- Receive Method ---
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(StartEmbeddingJob.class, this::canStartJob, msg -> handleStartEmbeddingJob(msg))
                .match(RangeReadComplete.class, this::isReading, msg -> handleRangeReadComplete(msg))
                .match(DbRecordWritten.class, this::isProgressRelevant, msg -> handleDbRecordWritten(msg)) // Use DbRecordWritten
                .match(DbRecordWriteFailed.class, this::isProgressRelevant, msg -> handleDbRecordWriteFailed(msg)) // Use DbRecordWriteFailed
                .match(Terminated.class, msg -> handleTerminated(msg))
                .match(InitiateChunkerShutdown.class, msg -> handleInitiateChunkerShutdown())
                .match(InitiateDbWriterShutdown.class, msg -> handleInitiateDbWriterShutdown())
                .matchAny(msg -> handleUnknownMessage(msg))
                .build();
    }
    // --- Predicates ---
    private boolean canStartJob(StartEmbeddingJob msg) { return status == JobStatus.IDLE; }
    private boolean isReading(Object msg){ return status == JobStatus.READING; }
    private boolean isProgressRelevant(Object msg) {
        // Allow progress messages until final writer shutdown starts
        return status == JobStatus.READING || status == JobStatus.STOPPING_READERS ||
                status == JobStatus.WAITING_BEFORE_CHUNKER_SHUTDOWN ||
                status == JobStatus.STOPPING_CHUNKERS || status == JobStatus.STOPPING_EMBEDDERS ||
                status == JobStatus.WAITING_BEFORE_WRITER_SHUTDOWN ||
                status == JobStatus.STOPPING_WRITERS;
    }


    // --- Message Handlers ---

    /**
     * Initializes the job, determines the ID range (potentially limiting it),
     * creates child actors, and starts the data readers.
     */
    private void handleStartEmbeddingJob(StartEmbeddingJob msg) {
        log.info("Received StartEmbeddingJob request from {}", getSender());
        this.originalRequester = getSender();
        this.jobStartTime = System.currentTimeMillis();
        this.status = JobStatus.READING;

        try {
            // 1. Determine FULL data range from DB
            log.info("Querying database for FULL min/max ID in raw_investment_data...");
            Integer minIdResult = jdbcTemplate.queryForObject("SELECT COALESCE(MIN(id), 1) FROM raw_investment_data", Integer.class);
            Integer maxIdResult = jdbcTemplate.queryForObject("SELECT COALESCE(MAX(id), 0) FROM raw_investment_data", Integer.class);
            int minId = (minIdResult != null) ? minIdResult : 1;
            int maxId = (maxIdResult != null) ? maxIdResult : 0; // Actual max ID in DB

            if (maxId < minId) {
                log.warn("raw_investment_data table appears to be empty. Job finishing.");
                this.totalRecordsToProcess = 0;
                finalizeJob(true, "No data found in raw_investment_data table.", Optional.empty());
                return;
            }
            long totalRecordsInDb = (long)maxId - minId + 1;
            log.info("Determined FULL data range: ID {} to {}. Total potential records: {}", minId, maxId, totalRecordsInDb);

            // --- Apply partial processing filter ---
            int effectiveMaxId = maxId; // Default to full range
            if (processPartialData && totalRecordsInDb > 0) {
                long targetRecordCount = Math.max(1, (long)(totalRecordsInDb * partialDataFraction)); // Ensure at least 1
                effectiveMaxId = minId + (int)targetRecordCount - 1;
                effectiveMaxId = Math.min(effectiveMaxId, maxId); // Clamp to actual max
                this.totalRecordsToProcess = (long)effectiveMaxId - minId + 1; // Use effective count for tracking
                log.warn("PARTIAL PROCESSING ENABLED ({}%). Effective range: [{}, {}]. Effective records: {}",
                        (int)(partialDataFraction * 100), minId, effectiveMaxId, this.totalRecordsToProcess);
            } else {
                this.totalRecordsToProcess = totalRecordsInDb; // Use full count
                log.info("Processing full data range [{}, {}]. Effective records: {}", minId, effectiveMaxId, this.totalRecordsToProcess);
            }
            // --- END Filter ---

            // 2. Create Child Actors (Using original, non-throttling setup)
            try {
                createChildActors();
            } catch (Exception childCreationEx) {
                log.error("Exception occurred DURING createChildActors", childCreationEx);
                failJob("Failed during child actor creation", Optional.of(childCreationEx));
                return;
            }

            // 3. Partition range and send initial messages (Use effectiveMaxId)
            try {
                if (effectiveMaxId < minId) { // Handle edge case where calculated max < min
                    log.warn("Effective max ID {} is less than min ID {}. No data to process.", effectiveMaxId, minId);
                    this.totalRecordsToProcess = 0; // Correct count
                    // Proceed to shutdown sequence as if readers finished immediately
                    checkIfReadersComplete();
                } else {
                    partitionAndStartReaders(minId, effectiveMaxId); // Use the potentially adjusted max ID
                }
            } catch (Exception partitionEx) {
                log.error("Exception occurred DURING partitionAndStartReaders", partitionEx);
                failJob("Failed during reader partitioning/start", Optional.of(partitionEx));
                return;
            }

            log.info("JobCoordinatorActor started successfully. Reading data (effective range)...");

        } catch (DataAccessException e) {
            log.error("Database error during job initialization", e);
            failJob("Initialization failed due to database error", Optional.of(e));
        } catch (Exception e) {
            log.error("Unexpected failure during job initialization phase", e);
            failJob("Initialization failed", Optional.of(e));
        }
    }

    /**
     * Handles completion notification from a DataReaderActor.
     */
    private void handleRangeReadComplete(RangeReadComplete msg) {
        log.info("DataReader {} reported completion.", msg.readerId());
        int remaining = activeReaders.decrementAndGet();
        log.info("Remaining active readers: {}", remaining);
        // Check if all readers are done to initiate sequential shutdown
        checkIfReadersComplete();
    }

    /**
     * Handles successful write notification from a DbWriterActor.
     */
    private void handleDbRecordWritten(DbRecordWritten msg) {
        long totalWritten = recordsWritten.addAndGet(msg.count());
        // Log progress periodically
        if (totalWritten % 1000 == 0 || (totalRecordsToProcess > 0 && totalWritten >= this.totalRecordsToProcess * 2)) { // Heuristic check
            log.info("Progress: {} records written (approx {} expected).", totalWritten, this.totalRecordsToProcess * 2); // Rough estimate
        }
    }

    /**
     * Handles failure notification from a DbWriterActor.
     */
    private void handleDbRecordWriteFailed(DbRecordWriteFailed msg) {
        log.error("Received DbRecordWriteFailed from {}: Reason: {}", getSender(), msg.reason(), msg.cause().orElse(null));
        // Immediately trigger job failure sequence
        failJob("Database write failed: " + msg.reason(), msg.cause());
    }

    /**
     * Handles Terminated messages from watched children to manage shutdown sequence.
     */
    private void handleTerminated(Terminated msg) {
        ActorRef terminatedActor = msg.actor();
        if (watchedChildren.remove(terminatedActor)) {
            log.info("Watched child terminated: {}. Remaining watched children: {}", terminatedActor.path().name(), watchedChildren.size());

            if (status == JobStatus.READING) {
                String failureReason = "Critical actor " + terminatedActor.path().name() + " terminated unexpectedly during READING phase";
                log.error(failureReason);
                failJob(failureReason, Optional.empty());
            }
            else if (status == JobStatus.STOPPING_READERS && isReader(terminatedActor)) {
                if (areAllReadersTerminated()) {
                    scheduleChunkerShutdown(); // Schedule next step
                }
            } else if (status == JobStatus.STOPPING_CHUNKERS && terminatedActor.equals(chunkerActor)) {
                initiateEmbeddingRouterShutdown(); // Immediate next step
            } else if (status == JobStatus.STOPPING_EMBEDDERS && terminatedActor.equals(embeddingRouter)) {
                scheduleDbWriterRouterShutdown(); // Schedule final step
            } else if (status == JobStatus.STOPPING_WRITERS && terminatedActor.equals(dbWriterRouter)) {
                checkIfAllChildrenTerminated(); // Check if job is fully complete
            } else if (status == JobStatus.STOPPING_FAILED) {
                checkIfAllChildrenTerminated(); // Check if failed cleanup is done
            }
            // Ignore Terminated during WAITING states
            else if (status == JobStatus.WAITING_BEFORE_CHUNKER_SHUTDOWN || status == JobStatus.WAITING_BEFORE_WRITER_SHUTDOWN) {
                log.debug("Ignoring termination of {} during a WAITING state.", terminatedActor.path().name());
            }
        } else {
            log.warn("Received Terminated message for non-watched or already removed actor: {}", terminatedActor);
        }
    }

    /**
     * Handles the scheduled trigger to initiate chunker shutdown.
     */
    private void handleInitiateChunkerShutdown() {
        if (status == JobStatus.WAITING_BEFORE_CHUNKER_SHUTDOWN) {
            log.info("Delay complete. Initiating Chunker shutdown sequence...");
            initiateChunkerShutdown(); // Now actually send PoisonPill
        } else {
            log.warn("Received InitiateChunkerShutdown in unexpected state: {}", status);
        }
    }

    /**
     * Handles the scheduled trigger to initiate DB writer shutdown.
     */
    private void handleInitiateDbWriterShutdown() {
        if (status == JobStatus.WAITING_BEFORE_WRITER_SHUTDOWN) {
            log.info("Delay complete. Initiating DbWriterRouter shutdown sequence...");
            sendPoisonPillToDbWriter();
        } else {
            log.warn("Received InitiateDbWriterShutdown in unexpected state: {}", status);
        }
    }

    private void handleUnknownMessage(Object msg) {
        if (status == JobStatus.COMPLETED || status == JobStatus.FAILED) {
            log.debug("Ignoring message {} as job is already finalized.", msg.getClass().getSimpleName());
            return;
        }
        log.warn("Received unknown message: {} from {}", msg.getClass().getName(), getSender());
    }

    // --- Helper Methods ---

    /**
     * Creates child actors using the original setup (before throttling).
     * Uses original 2-arg ChunkerConfiguration.
     * Does NOT pass chunkerRef to EmbeddingActor props.
     */
    private void createChildActors() {
        log.info("Creating child actors (non-throttled setup): N={}, P={}, M={}",
                jobConfig.numberOfReaders(), jobConfig.embeddingRouterPoolSize(), jobConfig.dbWriterRouterPoolSize());

        SpringAkkaExtension.SpringExt ext = SpringAkkaExtension.provider.get(getContext().getSystem());
        watchedChildren.clear();

        // --- Use original ChunkerConfiguration (2 args) ---
        ChunkerConfiguration chunkerConfig = new ChunkerConfiguration(
                jobConfig.chunkerTargetSize(),
                jobConfig.chunkerMinSize()
        );
        ReaderConfiguration readerConfig = new ReaderConfiguration(jobConfig.dbPageSize());

        String ioDispatcherName = Dispatchers.DefaultBlockingDispatcherId();
        log.info("Using Akka default blocking dispatcher: {}", ioDispatcherName);

        // Create DB Writer Router (Sequential version)
        Props dbWriterActorProps = ext.props("dbWriterActor", getSelf()).withDispatcher(ioDispatcherName);
        this.dbWriterRouter = getContext().actorOf(new RoundRobinPool(jobConfig.dbWriterRouterPoolSize()).props(dbWriterActorProps), "dbWriterRouter");
        getContext().watch(this.dbWriterRouter);
        watchedChildren.add(this.dbWriterRouter);
        log.info("Created DB Writer Router: {}", this.dbWriterRouter);

        // Create Embedding Router (Original props, no chunkerRef)
        Props embeddingActorProps = ext.props("embeddingActor", this.dbWriterRouter).withDispatcher(ioDispatcherName);
        this.embeddingRouter = getContext().actorOf(new RoundRobinPool(jobConfig.embeddingRouterPoolSize()).props(embeddingActorProps), "embeddingRouter");
        getContext().watch(this.embeddingRouter);
        watchedChildren.add(this.embeddingRouter);
        log.info("Created Embedding Router: {}", this.embeddingRouter);

        // Create Chunker Actor (Original config)
        Props chunkerProps = ext.props("chunkerActor", chunkerConfig, this.embeddingRouter);
        this.chunkerActor = getContext().actorOf(chunkerProps, "chunkerActor");
        getContext().watch(this.chunkerActor);
        watchedChildren.add(this.chunkerActor);
        log.info("Created Chunker Actor: {}", this.chunkerActor);

        // Create Data Readers
        this.activeReaders.set(jobConfig.numberOfReaders());
        log.debug("Creating {} DataReader actors...", jobConfig.numberOfReaders());
        for (int i = 0; i < jobConfig.numberOfReaders(); i++) {
            Props dataReaderProps = ext.props("dataReaderActor", i, readerConfig, this.chunkerActor, getSelf()).withDispatcher(ioDispatcherName);
            ActorRef reader = getContext().actorOf(dataReaderProps, "dataReader-" + i);
            getContext().watch(reader);
            watchedChildren.add(reader);
            log.debug("Created DataReader Actor: {}", reader);
        }
        log.info("Created and watching {} actors/routers.", watchedChildren.size());
    }

    /**
     * Calculates ID ranges for each reader based on minId and effectiveMaxId.
     */
    private void partitionAndStartReaders(int minId, int effectiveMaxId) {
        long totalIds = (long)effectiveMaxId - minId + 1;
        if (totalIds <= 0 || jobConfig.numberOfReaders() <= 0) {
            if (totalIds <= 0) log.warn("Effective ID range resulted in zero records to process.");
            else log.warn("Zero readers configured.");
            checkIfReadersComplete();
            return;
        }
        long idsPerReader = totalIds / jobConfig.numberOfReaders();
        long remainder = totalIds % jobConfig.numberOfReaders();
        int currentStartId = minId;
        int readersToStart = 0;

        List<ActorRef> currentReaders = new ArrayList<>();
        for(ActorRef child : watchedChildren) { if (isReader(child)) { currentReaders.add(child); } }
        currentReaders.sort((a1, a2) -> a1.path().name().compareTo(a2.path().name()));

        for (int i = 0; i < jobConfig.numberOfReaders(); i++) {
            long rangeSize = idsPerReader + (i < remainder ? 1 : 0);
            int currentEndId = (int)Math.min((long)currentStartId + rangeSize, (long)effectiveMaxId + 1);

            if (rangeSize > 0 && currentStartId < currentEndId && i < currentReaders.size()) {
                ActorRef reader = currentReaders.get(i);
                ReadDataRange readMsg = new ReadDataRange(currentStartId, currentEndId);
                log.info("Sending {} to {}", readMsg, reader);
                reader.tell(readMsg, getSelf());
                currentStartId = currentEndId;
                readersToStart++;
            } else if (i >= currentReaders.size()){
                log.error("Mismatch between configured readers ({}) and created readers ({})", jobConfig.numberOfReaders(), currentReaders.size());
                failJob("Reader actor creation/tracking mismatch", Optional.empty());
                return;
            } else {
                log.warn("Skipping reader {} as its calculated range size is <= 0 or startId >= endId.", i);
                ActorRef skippedReader = currentReaders.get(i);
                getContext().unwatch(skippedReader);
                watchedChildren.remove(skippedReader);
                activeReaders.decrementAndGet();
            }
        }
        if (readersToStart == 0 && totalIds > 0) {
            log.warn("No readers were started despite having data range.");
            failJob("Failed to start any readers.", Optional.empty());
        } else if (readersToStart < jobConfig.numberOfReaders()) {
            log.warn("Started {} readers, less than configured {}.", readersToStart, jobConfig.numberOfReaders());
            checkIfReadersComplete(); // Check immediately if some readers were skipped
        }
    }

    /**
     * Checks if all readers have completed and schedules the chunker shutdown.
     */
    private void checkIfReadersComplete() {
        if (status == JobStatus.READING && activeReaders.get() <= 0) {
            log.info("All data readers have completed. Proceeding to stop them...");
            status = JobStatus.STOPPING_READERS;
            log.info("Stopping completed DataReader actors...");
            new HashSet<>(watchedChildren).forEach(child -> {
                if (child != null && isReader(child)) {
                    log.debug("Stopping completed reader: {}", child);
                    getContext().stop(child);
                }
            });
            // If all readers were already removed/stopped, schedule next step immediately
            if (areAllReadersTerminated()) {
                scheduleChunkerShutdown();
            }
        }
    }

    /**
     * Schedules the chunker shutdown after a delay.
     */
    private void scheduleChunkerShutdown() {
        if (status == JobStatus.STOPPING_READERS && areAllReadersTerminated()) {
            status = JobStatus.WAITING_BEFORE_CHUNKER_SHUTDOWN;
            // Keep the delay from previous step, maybe reduce if only processing 30%?
            // Let's keep it 5 mins for now to be safe, can tune later.
            Duration chunkerShutdownDelay = Duration.ofMinutes(5);
            log.info("All readers terminated. Scheduling Chunker shutdown in {}...", chunkerShutdownDelay);

            getContext().getSystem().scheduler().scheduleOnce(
                    chunkerShutdownDelay, getSelf(), new InitiateChunkerShutdown(),
                    getContext().getDispatcher(), ActorRef.noSender()
            );
        } else {
            log.warn("Attempted to schedule chunker shutdown from unexpected state: {} or not all readers terminated yet.", status);
        }
    }

    /**
     * Sends PoisonPill to the ChunkerActor. Called after delay.
     */
    private void initiateChunkerShutdown() {
        if (status == JobStatus.WAITING_BEFORE_CHUNKER_SHUTDOWN) {
            log.info("Initiating Chunker shutdown after delay.");
            status = JobStatus.STOPPING_CHUNKERS;
            if (chunkerActor != null) {
                log.info("Sending PoisonPill to ChunkerActor...");
                chunkerActor.tell(akka.actor.PoisonPill.getInstance(), ActorRef.noSender());
            } else {
                log.warn("Chunker actor ref was null when trying to initiate its shutdown.");
                initiateEmbeddingRouterShutdown(); // Proceed if already gone
            }
        } else {
            log.warn("Attempted to initiate chunker shutdown from unexpected state: {}", status);
        }
    }

    /**
     * Sends PoisonPill to the EmbeddingRouter. Called after Chunker terminates.
     */
    private void initiateEmbeddingRouterShutdown() {
        if (status == JobStatus.STOPPING_CHUNKERS) {
            log.info("Chunker terminated. Initiating EmbeddingRouter shutdown...");
            status = JobStatus.STOPPING_EMBEDDERS;
            if (embeddingRouter != null) {
                log.info("Sending PoisonPill to EmbeddingRouter...");
                embeddingRouter.tell(akka.actor.PoisonPill.getInstance(), ActorRef.noSender());
            } else {
                log.warn("Embedding router ref was null when trying to initiate its shutdown.");
                scheduleDbWriterRouterShutdown();
            }
        } else {
            log.warn("Attempted to initiate embedding router shutdown from unexpected state: {}", status);
        }
    }

    /**
     * Schedules the DbWriterRouter shutdown. Called after EmbeddingRouter terminates.
     */
    private void scheduleDbWriterRouterShutdown() {
        if (status == JobStatus.STOPPING_EMBEDDERS) {
            log.info("EmbeddingRouter terminated. Scheduling DbWriterRouter shutdown...");
            status = JobStatus.WAITING_BEFORE_WRITER_SHUTDOWN;
            Duration writerShutdownDelay = Duration.ofSeconds(2);
            log.info("Scheduling DbWriterRouter shutdown in {}", writerShutdownDelay);
            getContext().getSystem().scheduler().scheduleOnce(
                    writerShutdownDelay, getSelf(), new InitiateDbWriterShutdown(),
                    getContext().getDispatcher(), ActorRef.noSender()
            );
        } else {
            log.warn("Attempted to schedule DB writer router shutdown from unexpected state: {}", status);
        }
    }

    /**
     * Sends PoisonPill to the DbWriterRouter. Called after delay.
     */
    private void sendPoisonPillToDbWriter() {
        if (status == JobStatus.WAITING_BEFORE_WRITER_SHUTDOWN) {
            log.info("Sending PoisonPill to DbWriterRouter...");
            status = JobStatus.STOPPING_WRITERS;
            if (dbWriterRouter != null) {
                dbWriterRouter.tell(akka.actor.PoisonPill.getInstance(), ActorRef.noSender());
            } else {
                log.warn("DB writer router ref was null when trying to send PoisonPill.");
                checkIfAllChildrenTerminated();
            }
        } else {
            log.warn("Attempted to send PoisonPill to DB writer router from unexpected state: {}", status);
        }
    }

    private boolean isReader(ActorRef actorRef) { return actorRef != null && actorRef.path().name().startsWith("dataReader-"); }
    private boolean areAllReadersTerminated() { return watchedChildren.stream().noneMatch(this::isReader); }

    /**
     * Checks if all initially watched children have terminated. If so, finalizes the job.
     */
    private void checkIfAllChildrenTerminated() {
        if ((status == JobStatus.STOPPING_WRITERS || status == JobStatus.STOPPING_FAILED) && watchedChildren.isEmpty()) {
            if (status == JobStatus.STOPPING_WRITERS) {
                log.info("All watched children have terminated after sequential shutdown. Finalizing job as COMPLETED.");
                finalizeJob(true, "All stages completed successfully.", Optional.empty());
            } else {
                log.info("All watched children have terminated after failure detected. Finalizing job as FAILED.");
                finalizeJob(false, "Job failed: " + (firstFailure != null ? firstFailure.getMessage() : "Unknown reason"), Optional.ofNullable(firstFailure));
            }
        } else if (status == JobStatus.STOPPING_READERS || status == JobStatus.WAITING_BEFORE_CHUNKER_SHUTDOWN || status == JobStatus.STOPPING_CHUNKERS ||
                status == JobStatus.STOPPING_EMBEDDERS || status == JobStatus.WAITING_BEFORE_WRITER_SHUTDOWN ||
                status == JobStatus.STOPPING_WRITERS || status == JobStatus.STOPPING_FAILED) {
            log.debug("Waiting for remaining {} watched children to terminate.", watchedChildren.size());
        }
    }


    /**
     * Initiates the job failure sequence.
     */
    private void failJob(String reason, Optional<Throwable> cause) {
        if (status == JobStatus.FAILED || status == JobStatus.COMPLETED || status == JobStatus.STOPPING_FAILED) { /* ... ignore ... */ return; }
        JobStatus previousStatus = this.status;
        this.status = JobStatus.STOPPING_FAILED;
        if (cause.isPresent() && firstFailure == null) { this.firstFailure = cause.get(); }
        log.error("Job failing due to: {}", reason, cause.orElse(null));
        if (previousStatus == JobStatus.IDLE || previousStatus == JobStatus.READING) {
            log.info("Stopping all watched children immediately due to failure...");
            new HashSet<>(watchedChildren).forEach(child -> {
                if (child != null) {
                    log.debug("Stopping child: {}", child);
                    getContext().unwatch(child);
                    getContext().stop(child);
                    watchedChildren.remove(child);
                }
            });
            checkIfAllChildrenTerminated();
        } else {
            log.info("Failure occurred during sequential shutdown (state {}). Waiting for remaining children to terminate.", previousStatus);
        }
    }

    /**
     * Sends the final status message, logs summary, and stops self.
     */
    private void finalizeJob(boolean success, String reason, Optional<Throwable> cause) {
        if (status == JobStatus.COMPLETED || status == JobStatus.FAILED) { return; }
        long duration = System.currentTimeMillis() - jobStartTime;
        ActorRef replyTarget = this.originalRequester;
        JobStatus finalStatus;

        // --- Log final counts ---
        log.info("--- Job Finalization Summary ---");
        // Use the potentially adjusted totalRecordsToProcess for accuracy
        log.info("Effective Records To Process (based on actual range): {}", this.totalRecordsToProcess);
        log.info("Total Records Written (reported by DbWriterActor): {}", recordsWritten.get());
        log.info("-------------------------------");

        if (success) {
            finalStatus = JobStatus.COMPLETED;
            log.info(">>> Embedding Job COMPLETED Successfully in {} ms <<<", duration);
            if (replyTarget != null && !replyTarget.isTerminated()) {
                replyTarget.tell(new JobCompleted(duration), getSelf());
            } else if (replyTarget != null) {
                log.warn("Original requester {} terminated before final reply could be sent.", replyTarget);
            }
        } else {
            finalStatus = JobStatus.FAILED;
            log.error(">>> Embedding Job FAILED after {} ms. Reason: {} <<<", duration, reason, cause.orElse(null));
            if (replyTarget != null && !replyTarget.isTerminated()) {
                replyTarget.tell(new JobFailed(reason, cause, duration), getSelf());
            } else if (replyTarget != null) {
                log.warn("Original requester {} terminated before final reply could be sent.", replyTarget);
            }
        }
        this.status = finalStatus;
        log.info("JobCoordinatorActor stopping itself.");
        try { getContext().stop(getSelf()); } catch (Exception e) {
            log.warn("Exception while stopping JobCoordinatorActor (potentially already stopped): {}", e.getMessage(), e);
        }
    }
}