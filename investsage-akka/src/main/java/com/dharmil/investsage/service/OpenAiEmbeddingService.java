package com.dharmil.investsage.service; // Your package

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.embedding.Embedding;
import org.springframework.ai.embedding.EmbeddingResponse;
import org.springframework.ai.openai.OpenAiEmbeddingModel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class OpenAiEmbeddingService {

    private static final Logger log = LoggerFactory.getLogger(OpenAiEmbeddingService.class);

    private final OpenAiEmbeddingModel embeddingModel;
    private final int expectedDimension = 1536;

    @Autowired
    public OpenAiEmbeddingService(OpenAiEmbeddingModel embeddingModel) {
        this.embeddingModel = embeddingModel;
    }

    @PostConstruct
    private void checkInjection() {
        if (this.embeddingModel != null) {
            log.info("Spring AI OpenAiEmbeddingModel initialized. Class: {}", embeddingModel.getClass().getName());
        } else {
            log.error("FATAL: Spring AI OpenAiEmbeddingModel failed to inject! Check configuration and dependencies.");
        }
    }

    // --- GET SINGLE EMBEDDING ---
    public List<Double> getEmbedding(String text) {
        // --- ADDED: Log entry ---
        log.debug("SERVICE: getEmbedding called for text snippet: {}", text == null ? "NULL" : text.substring(0, Math.min(text.length(), 50)));

        if (text == null || text.trim().isEmpty()) {
            log.warn("SERVICE: Attempting to embed empty or null text. Returning zero vector.");
            return Collections.nCopies(expectedDimension, 0.0);
        }
        try {
            // --- ADDED: Log before API call ---
            log.debug("SERVICE: Calling Spring AI embeddingModel.embed()...");
            long startTime = System.nanoTime();
            float[] embeddingArray = embeddingModel.embed(text); // The actual API call via Spring AI
            long endTime = System.nanoTime();
            // --- ADDED: Log after API call ---
            log.debug("SERVICE: Spring AI embeddingModel.embed() returned {} element(s) in {} ms.",
                    (embeddingArray == null ? "null" : embeddingArray.length),
                    (endTime - startTime) / 1_000_000);


            if (embeddingArray == null || embeddingArray.length == 0) {
                log.error("SERVICE: OpenAI API returned null or empty embedding array for single text.");
                // Consider maybe throwing an exception here if this shouldn't happen
                return Collections.nCopies(expectedDimension, 0.0); // Fallback
            }
            if (embeddingArray.length != expectedDimension) {
                log.warn("SERVICE: OpenAI API returned embedding with unexpected dimension {} for single text (expected {}).",
                        embeddingArray.length, expectedDimension);
                // Consider throwing exception?
                return Collections.nCopies(expectedDimension, 0.0); // Fallback
            }

            log.trace("SERVICE: Successfully received single embedding dimension: {}", embeddingArray.length);
            List<Double> embeddingList = new ArrayList<>(embeddingArray.length);
            for (float f : embeddingArray) {
                embeddingList.add((double) f);
            }
            return embeddingList;

        } catch (Exception e) {
            // --- ADDED: Log exception origin ---
            log.error("SERVICE: Exception during Spring AI embeddingModel.embed (single text): {}", e.getMessage(), e);
            // This service method returns fallback on error, actor should check/throw
            return Collections.nCopies(expectedDimension, 0.0);
        }
    }

    // --- GET BATCH EMBEDDINGS ---
    public List<List<Double>> getEmbeddings(List<String> texts) {
        // --- ADDED: Log entry ---
        int initialSize = (texts == null) ? 0 : texts.size();
        log.debug("SERVICE: getEmbeddings called for batch of initial size {}.", initialSize);

        if (texts == null || texts.isEmpty()) {
            log.warn("SERVICE: Attempting to embed empty or null text list.");
            return Collections.emptyList();
        }

        // Filter out empty texts BEFORE sending to API
        List<String> textsToEmbed = texts.stream()
                .filter(text -> text != null && !text.trim().isEmpty())
                .collect(Collectors.toList());

        if (textsToEmbed.isEmpty()) {
            log.warn("SERVICE: All texts in the batch were empty or null after cleaning.");
            // Return list of zero vectors matching original size
            return texts.stream()
                    .map(t -> Collections.nCopies(expectedDimension, 0.0))
                    .collect(Collectors.toList());
        }

        try {
            // --- ADDED: Log before API call ---
            log.debug("SERVICE: Calling Spring AI embeddingModel.embedForResponse() for {} non-empty texts...", textsToEmbed.size());
            long startTime = System.nanoTime();
            EmbeddingResponse embeddingResponse = embeddingModel.embedForResponse(textsToEmbed);
            long endTime = System.nanoTime();
            int resultsCount = (embeddingResponse == null || embeddingResponse.getResults() == null) ? 0 : embeddingResponse.getResults().size();
            // --- ADDED: Log after API call ---
            log.debug("SERVICE: Spring AI embeddingModel.embedForResponse() returned {} result(s) in {} ms.",
                    resultsCount, (endTime - startTime) / 1_000_000);


            if (embeddingResponse == null || embeddingResponse.getResults() == null) {
                log.error("SERVICE: Spring AI returned null response or null results for batch embedding.");
                // Fallback: return zero vectors for all original texts
                return texts.stream().map(t -> Collections.nCopies(expectedDimension, 0.0)).collect(Collectors.toList());
            }

            List<Embedding> embeddingResults = embeddingResponse.getResults();
            if (embeddingResults.size() != textsToEmbed.size()) {
                log.error("SERVICE: Mismatch between requested non-empty batch size ({}) and embeddings returned ({}).", textsToEmbed.size(), embeddingResults.size());
                // Fallback strategy might be complex - returning zeros for simplicity
                return texts.stream().map(t -> Collections.nCopies(expectedDimension, 0.0)).collect(Collectors.toList());
            }

            // Convert results
            List<List<Double>> batchEmbeddings = new ArrayList<>(textsToEmbed.size());
            for (int i=0; i<embeddingResults.size(); i++) {
                Embedding embeddingResult = embeddingResults.get(i);
                if (embeddingResult == null || embeddingResult.getOutput() == null) {
                    log.warn("SERVICE: Null embedding or output found at index {} in batch response.", i);
                    batchEmbeddings.add(Collections.nCopies(expectedDimension, 0.0)); // Add fallback
                    continue;
                }

                float[] outputVector = embeddingResult.getOutput();
                if (outputVector.length == 0) {
                    log.warn("SERVICE: Received empty embedding array at index {} within batch response.", i);
                    batchEmbeddings.add(Collections.nCopies(expectedDimension, 0.0)); // Add fallback
                } else if (outputVector.length != expectedDimension) {
                    log.warn("SERVICE: OpenAI API returned embedding with unexpected dimension {} at index {} in batch (expected {}).",
                            outputVector.length, i, expectedDimension);
                    batchEmbeddings.add(Collections.nCopies(expectedDimension, 0.0)); // Add fallback
                } else {
                    List<Double> embeddingList = new ArrayList<>(outputVector.length);
                    for (float f : outputVector) {
                        embeddingList.add((double) f);
                    }
                    batchEmbeddings.add(embeddingList);
                }
            }

            // Reconstruct the result list to match the original input list structure (including nulls/empties)
            List<List<Double>> finalResult = new ArrayList<>(texts.size());
            int batchResultIndex = 0;
            for (String originalText : texts) {
                if (originalText == null || originalText.trim().isEmpty()) {
                    finalResult.add(Collections.nCopies(expectedDimension, 0.0));
                } else {
                    // This assumes the order returned matches the filtered textsToEmbed list
                    if (batchResultIndex < batchEmbeddings.size()) {
                        finalResult.add(batchEmbeddings.get(batchResultIndex++));
                    } else {
                        log.error("SERVICE: Index out of bounds during final reconstruction ({} >= {}). Should not happen if counts matched.", batchResultIndex, batchEmbeddings.size());
                        finalResult.add(Collections.nCopies(expectedDimension, 0.0)); // Fallback
                    }
                }
            }
            log.trace("SERVICE: Successfully processed batch embeddings for {} initial texts.", texts.size());
            return finalResult;

        } catch (Exception e) {
            // --- ADDED: Log exception origin ---
            log.error("SERVICE: Exception during Spring AI embeddingModel.embedForResponse (batch): {}", e.getMessage(), e);
            // Fallback: return zero vectors for all original texts
            return texts.stream().map(t -> Collections.nCopies(expectedDimension, 0.0)).collect(Collectors.toList());
        }
    }
}