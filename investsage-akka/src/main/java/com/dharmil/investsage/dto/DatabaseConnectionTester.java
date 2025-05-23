package com.dharmil.investsage.dto;

import jakarta.annotation.PostConstruct; // Correct import
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component // Mark this as a Spring component to be managed
public class DatabaseConnectionTester {

    private static final Logger log = LoggerFactory.getLogger(DatabaseConnectionTester.class);

    private final JdbcTemplate jdbcTemplate;

    @Autowired // Inject Spring's JdbcTemplate
    public DatabaseConnectionTester(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @PostConstruct // Run this method after the bean is initialized
    public void testConnection() {
        log.info("Testing database connection...");
        try {
            // Perform a simple query to check connectivity
            Integer result = jdbcTemplate.queryForObject("SELECT 1", Integer.class);
            if (result != null && result == 1) {
                log.info("Database connection test successful! (SELECT 1 returned 1)");
            } else {
                log.warn("Database connection test query executed, but didn't return expected result 1.");
            }
            // Test if pgvector extension is accessible (optional)
            String vectorVersion = jdbcTemplate.queryForObject("SELECT extversion FROM pg_extension WHERE extname = 'vector'", String.class);
            log.info("Detected pgvector extension version: {}", vectorVersion);

        } catch (Exception e) {
            log.error("Database connection test FAILED: {}", e.getMessage(), e);
        }
    }
}