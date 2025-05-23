package com.dharmil.investsage.controller;

import com.dharmil.investsage.service.RagDataProcessor; // Ensure correct import
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/v1/investment")
public class InvestmentQueryController {

    private static final Logger log = LoggerFactory.getLogger(InvestmentQueryController.class);

    // Inject the cleaned-up RagDataProcessor (now only does retrieval)
    private final RagDataProcessor ragDataProcessor;

    @Autowired
    public InvestmentQueryController(@Lazy RagDataProcessor ragDataProcessor) {
        this.ragDataProcessor = ragDataProcessor;
    }

    // Inner class for request body structure
    public static class QueryRequest {
        private String query;
        private Integer limit;
        // Getters & Setters
        public String getQuery() { return query; }
        public void setQuery(String query) { this.query = query; }
        public Integer getLimit() { return limit; }
        public void setLimit(Integer limit) { this.limit = limit; }
    }

    @PostMapping("/query")
    public ResponseEntity<List<String>> findRelevantChunks(@RequestBody QueryRequest request) {
        if (request == null || request.getQuery() == null || request.getQuery().trim().isEmpty()) {
            log.warn("Received empty or invalid query request.");
            return ResponseEntity.badRequest().body(List.of("Query cannot be empty."));
        }

        int limit = (request.getLimit() != null && request.getLimit() > 0) ? request.getLimit() : 5;

        log.info("Received query request (targeting new_investment_embeddings): '{}', limit: {}", request.getQuery(), limit);

        try {
            // Call the findSimilarChunks method (which queries new_investment_embeddings)
            List<String> relevantChunks = ragDataProcessor.findSimilarChunks(request.getQuery(), limit);
            log.info("Returning {} relevant chunks for query from new_investment_embeddings.", relevantChunks.size());
            return ResponseEntity.ok(relevantChunks);
        } catch (Exception e) {
            log.error("Error processing query request against new_investment_embeddings: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(List.of("Error processing your query."));
        }
    }
}