package com.dharmil.investsage.runner;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.pattern.Patterns;
import com.dharmil.investsage.akka.actor.JobCoordinatorActor;
import com.dharmil.investsage.akka.messages.JobCompleted;
import com.dharmil.investsage.akka.messages.JobFailed;
import com.dharmil.investsage.akka.messages.StartEmbeddingJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

/**
 * Initiates the Akka-based embedding generation job on application startup.
 * Replaces the previous Spring Batch DataEmbeddingRunner.
 */
@Component
@Profile("!test") // Optional: Prevent running during unit/integration tests
public class AkkaEmbeddingJobRunner implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(AkkaEmbeddingJobRunner.class);

    // Flag to control running the embedding job on startup
    // SET TO true TO RUN ONCE, THEN SET TO false (or use profiles/config)
    private final boolean runAkkaEmbeddingJob = false; // <<< CHANGE THIS TO true TO RUN

    private final ActorSystem actorSystem;
    // Remove unused dependencies if they are truly not needed elsewhere in this class
    // private final JdbcTemplate jdbcTemplate;
    // private final OpenAiEmbeddingService embeddingService;

    @Autowired
    // Simplified constructor if jdbcTemplate/embeddingService are not needed here
    public AkkaEmbeddingJobRunner(ActorSystem actorSystem /*, JdbcTemplate jdbcTemplate, OpenAiEmbeddingService embeddingService */) {
        this.actorSystem = actorSystem;
        // this.jdbcTemplate = jdbcTemplate;
        // this.embeddingService = embeddingService;
    }

    @Override
    public void run(String... args) throws Exception {
        if (runAkkaEmbeddingJob) {
            log.warn("========== CommandLineRunner: Launching Akka Embedding Job ==========");

            // Updated JobConfiguration instantiation - removed last two arguments
            JobCoordinatorActor.JobConfiguration jobConfig = new JobCoordinatorActor.JobConfiguration(
                    4,     // numberOfReaders (N)
                    6,     // embeddingRouterPoolSize (P)
                    4,     // dbWriterRouterPoolSize (M)
                    100,   // dbPageSize
                    500,   // chunkerTargetSize
                    50     // chunkerMinSize
            );
            log.info("Using Job Configuration: {}", jobConfig);


            // Create the JobCoordinatorActor (remains the same)
            ActorRef coordinator = actorSystem.actorOf(
                    JobCoordinatorActor.props(actorSystem, jobConfig),
                    "embeddingJobCoordinator"
            );
            log.info("Created JobCoordinatorActor: {}", coordinator);

            // Define Ask Timeout (remains the same)
            final Duration askTimeout = Duration.ofHours(4);

            // Send StartEmbeddingJob message (remains the same)
            log.info("Sending StartEmbeddingJob message to coordinator...");
            CompletionStage<Object> futureResult = Patterns.ask(
                    coordinator,
                    new StartEmbeddingJob(ActorRef.noSender()),
                    askTimeout
            );

            // Handle the result asynchronously - Updated block
            futureResult.whenComplete((result, error) -> {
                if (error != null) {
                    log.error("!!!!!!!!!! Ask pattern failed for embedding job !!!!!!!!!!", error);
                    // Application continues running, but log the ask failure.
                    // Depending on requirements, you might want other error handling.
                } else {
                    // Process the reply message from the coordinator (remains the same)
                    if (result instanceof JobCompleted completed) {
                        log.info("========== Akka Embedding Job COMPLETED Successfully ==========");
                        log.info("Total duration reported by coordinator: {} ms", completed.durationMillis());
                        log.warn("========== IMPORTANT: Set 'runAkkaEmbeddingJob' to false in AkkaEmbeddingJobRunner after successful run! ==========");
                    } else if (result instanceof JobFailed failed) {
                        log.error("!!!!!!!!!! Akka Embedding Job FAILED !!!!!!!!!!");
                        log.error("Reason: {}", failed.reason());
                        failed.cause().ifPresent(ex -> log.error("Underlying cause:", ex));
                        log.error("Duration before failure: {} ms", failed.durationMillis());
                    } else {
                        log.error("Received unexpected result from JobCoordinatorActor: {}", result.getClass().getName());
                    }
                }

                // --- REMOVED ActorSystem termination ---
                // log.info("Job finished, shutting down ActorSystem..."); // Removed
                // actorSystem.terminate(); // Removed
                log.info("Akka Job Future completed (Success or Failure). ActorSystem lifecycle managed by Spring.");
                // The ActorSystem will now be terminated when the Spring context shuts down.
            });

            log.info("Akka Embedding Job launch initiated. Waiting for completion (async)...");
            // Application continues running...

        } else {
            log.info("CommandLineRunner: Akka Embedding Job is disabled (runAkkaEmbeddingJob=false).");
        }
    }
}