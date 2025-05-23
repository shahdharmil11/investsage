package com.dharmil.investsage.runner;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier; // Use if multiple Jobs exist
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class DataEmbeddingRunner implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(DataEmbeddingRunner.class);

    @Autowired
    private JobLauncher jobLauncher; // Injected by Spring Boot Batch auto-configuration

    @Autowired
    // Autowire the specific Job bean we defined in the configuration
    @Qualifier("embeddingJob") // Use qualifier if bean name might clash
    private Job embeddingJob;

    // Flag to control running the embedding job on startup
    // SET TO true TO RUN ONCE, THEN SET TO false
    private final boolean runBatchEmbeddingJob = false;

    @Override
    public void run(String... args) throws Exception {
        if (runBatchEmbeddingJob) {
            log.warn("========== CommandLineRunner: Launching Spring Batch Embedding Job ==========");

            // Log bean state before launch
            if (jobLauncher == null) {
                log.error("JobLauncher bean is NULL. Check Spring Batch configuration.");
                return;
            }
            if (embeddingJob == null) {
                log.error("EmbeddingJob bean is NULL. Check Spring Batch configuration.");
                return;
            }
            log.info("JobLauncher and EmbeddingJob beans seem to be injected correctly.");

            try {
                // Add a timestamp parameter to ensure the job instance is unique each time
                // This allows the job to be re-runnable using RunIdIncrementer
                JobParameters jobParameters = new JobParametersBuilder()
                        .addLong("startTime", System.currentTimeMillis())
                        .toJobParameters();

                // Launch the job using the injected launcher and job beans
                jobLauncher.run(embeddingJob, jobParameters);

                // Note: Job execution is often asynchronous depending on the task executor.
                // This log message indicates the launch attempt finished, not necessarily the entire job.
                log.info("Spring Batch Embedding job launched successfully.");

            } catch (Exception e) {
                log.error("!!!!!!!!!! Error launching or executing Spring Batch Embedding Job !!!!!!!!!!", e);
            }
            log.warn("========== CommandLineRunner: Spring Batch Job launch attempt complete ==========");
            log.warn("========== IMPORTANT: Set 'runBatchEmbeddingJob' to false in DataEmbeddingRunner after successful run! ==========");

        } else {
            log.info("CommandLineRunner: Spring Batch Embedding Job is disabled (runBatchEmbeddingJob=false).");
        }
    }
}