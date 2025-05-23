package com.dharmil.investsage.config;

import com.dharmil.investsage.batch.*; // Import POJOs and components
import com.dharmil.investsage.service.OpenAiEmbeddingService;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
// import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor; // Alt Task Executor
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;


import javax.sql.DataSource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
// No need for @EnableBatchProcessing here if it's on the main Application class
public class EmbeddingBatchConfiguration {

    // Autowire necessary beans provided by Spring Boot / @EnableBatchProcessing
    @Autowired private DataSource dataSource;
    @Autowired private JobRepository jobRepository;
    @Autowired private PlatformTransactionManager transactionManager;

    // Autowire your custom services needed by batch components
    @Autowired private OpenAiEmbeddingService embeddingService;
    @Autowired private JdbcTemplate jdbcTemplate;

    // --- Reader Bean ---
    @Bean
    public JdbcPagingItemReader<RawDataRecord> embeddingReader() {
        // Configure page size (rows read per DB query per thread)
        // Adjust based on expected row size and memory
        int pageSize = 100;
        return new JdbcPagingItemReaderBuilder<RawDataRecord>()
                .name("embeddingRawDataReader") // Unique name for the reader bean
                .dataSource(dataSource)
                .queryProvider(createQueryProvider()) // Use the bean defined below
                .pageSize(pageSize)
                .rowMapper(new BeanPropertyRowMapper<>(RawDataRecord.class)) // Maps DB columns to POJO fields
                .saveState(false) // Generally false for non-restartable readers in parallel steps
                .build();
    }

    // --- Query Provider Bean for Reader ---
    // Defines the SQL query for the paging reader
    @Bean
    public PagingQueryProvider createQueryProvider() {
        SqlPagingQueryProviderFactoryBean factory = new SqlPagingQueryProviderFactoryBean();
        factory.setDataSource(dataSource);
        factory.setSelectClause("SELECT id, raw_text");
        factory.setFromClause("FROM raw_investment_data");
        // Sort key MUST be unique for reliable paging, typically the primary key
        Map<String, Order> sortKeys = new HashMap<>();
        sortKeys.put("id", Order.ASCENDING);
        factory.setSortKeys(sortKeys);
        try {
            //getObject() creates the PagingQueryProvider instance
            return factory.getObject();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create PagingQueryProvider", e);
        }
    }

    // --- Processor Bean ---
    // Creates an instance of your ItemProcessor implementation
    @Bean
    public ItemProcessor<RawDataRecord, List<ChunkInfo>> embeddingProcessor() {
        return new RawDataToChunksProcessor();
    }

    // --- Writer Bean ---
    // Creates an instance of your custom ItemWriter implementation
    @Bean
    public ItemWriter<List<ChunkInfo>> embeddingWriter() {
        return new EmbeddingItemWriter(embeddingService, jdbcTemplate); // Inject dependencies
    }

    // --- Task Executor Bean for Parallelism ---
    // Defines the thread pool for parallel step execution
    @Bean
    public TaskExecutor batchTaskExecutor() {
        SimpleAsyncTaskExecutor asyncTaskExecutor = new SimpleAsyncTaskExecutor("batch_");
        // IMPORTANT: Tune concurrency based on:
        // - Number of CPU cores
        // - OpenAI API rate limits (concurrent requests allowed)
        // - Database connection pool size and capacity
        // Start with a moderate number (e.g., 4 or 8) and monitor performance/errors.
        asyncTaskExecutor.setConcurrencyLimit(4);
        return asyncTaskExecutor;
    }

    // --- Step Bean ---
    // Defines the main processing step (Read -> Process -> Write)
    @Bean
    public Step embeddingStep(JdbcPagingItemReader<RawDataRecord> reader, // Inject beans by type
                              ItemProcessor<RawDataRecord, List<ChunkInfo>> processor,
                              ItemWriter<List<ChunkInfo>> writer,
                              @Qualifier("batchTaskExecutor") TaskExecutor taskExecutor) {
        // Step Chunk Size: How many items (RawDataRecord) are processed together
        // by one thread before the writer is called for that thread's results.
        // Affects transaction size, memory, and recovery granularity.
        int stepChunkSize = 10;
        return new StepBuilder("embeddingProcessingStep", jobRepository)
                // Define input <I> and output <O> types for the chunk
                .<RawDataRecord, List<ChunkInfo>>chunk(stepChunkSize, transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .taskExecutor(taskExecutor) // Enable parallel execution of chunks
                // Optional: Define fault tolerance (retry/skip logic) here
                // .faultTolerant().skipLimit(10).skip(Exception.class)...
                .build();
    }

    // --- Job Bean ---
    // Defines the overall batch job
    @Bean
    public Job embeddingJob(Step embeddingStep) { // Inject the Step bean
        return new JobBuilder("generateEmbeddingsJob", jobRepository)
                // Allows re-running the job with different parameters (e.g., timestamp)
                .incrementer(new RunIdIncrementer())
                .flow(embeddingStep) // Define the sequence of steps (only one here)
                .end()
                .build();
    }
}