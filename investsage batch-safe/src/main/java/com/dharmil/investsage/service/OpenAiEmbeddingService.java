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

    public List<Double> getEmbedding(String text) {
        // Removed null check based on IntelliJ suggestion
        if (text == null || text.trim().isEmpty()) {
            log.warn("Attempting to embed empty or null text. Returning zero vector.");
            return Collections.nCopies(expectedDimension, 0.0);
        }
        try {
            log.debug("Requesting single embedding for text snippet: {}", text.substring(0, Math.min(text.length(), 50)));
            float[] embeddingArray = embeddingModel.embed(text);

            // Keep the check for empty array length
            if (embeddingArray.length == 0) {
                log.error("OpenAI API returned empty embedding array for single text.");
                return Collections.nCopies(expectedDimension, 0.0);
            }
            if (embeddingArray.length != expectedDimension) {
                log.warn("OpenAI API returned embedding with unexpected dimension {} for single text", embeddingArray.length);
                return Collections.nCopies(expectedDimension, 0.0);
            }

            log.trace("Successfully received single embedding dimension: {}", embeddingArray.length);
            List<Double> embeddingList = new ArrayList<>(embeddingArray.length);
            for (float f : embeddingArray) {
                embeddingList.add((double) f);
            }
            return embeddingList;

        } catch (Exception e) {
            log.error("Error calling Spring AI OpenAiEmbeddingModel.embed (single text): {}", e.getMessage(), e);
            return Collections.nCopies(expectedDimension, 0.0);
        }
    }

    public List<List<Double>> getEmbeddings(List<String> texts) {
        // Removed null check based on IntelliJ suggestion
        if (texts == null || texts.isEmpty()) {
            log.warn("Attempting to embed empty or null text list.");
            return Collections.emptyList();
        }

        List<String> textsToEmbed = new ArrayList<>(texts);
        textsToEmbed.removeIf(text -> text == null || text.trim().isEmpty());
        if (textsToEmbed.isEmpty()) {
            log.warn("All texts in the batch were empty or null after cleaning.");
            return texts.stream().map(t -> Collections.nCopies(expectedDimension, 0.0)).collect(Collectors.toList());
        }

        try {
            log.debug("Requesting batch embeddings for {} texts...", textsToEmbed.size());
            EmbeddingResponse embeddingResponse = embeddingModel.embedForResponse(textsToEmbed);

            // Removed redundant null checks based on IntelliJ suggestion
            // if (embeddingResponse == null || embeddingResponse.getResults() == null) { ... }

            List<List<Double>> batchEmbeddings = new ArrayList<>();
            // Use .getResults() directly, assuming it's non-null
            for (Embedding embeddingResult : embeddingResponse.getResults()) {
                // Removed null check for embeddingResult itself if IntelliJ guarantees it

                float[] outputVector = embeddingResult.getOutput();

                // Removed redundant 'outputVector == null' check based on IntelliJ suggestion
                // Keep the check for empty array length
                if (outputVector.length == 0) {
                    log.warn("Received empty embedding array within batch response.");
                    batchEmbeddings.add(Collections.nCopies(expectedDimension, 0.0));
                    continue;
                }

                if (outputVector.length != expectedDimension) {
                    log.warn("OpenAI API returned embedding with unexpected dimension {} in batch", outputVector.length);
                    batchEmbeddings.add(Collections.nCopies(expectedDimension, 0.0));
                } else {
                    List<Double> embeddingList = new ArrayList<>(outputVector.length);
                    for (float f : outputVector) {
                        embeddingList.add((double) f);
                    }
                    batchEmbeddings.add(embeddingList);
                }
            }

            // (Rest of the size checking and reconstruction logic remains the same)
            if (batchEmbeddings.size() != textsToEmbed.size()) {
                log.error("Mismatch between requested non-empty batch size ({}) and processed embeddings size ({}). Check API response details.", textsToEmbed.size(), batchEmbeddings.size());
                List<List<Double>> fallbackResult = new ArrayList<>(texts.size());
                int currentBatchIdx = 0;
                for(String originalText : texts) {
                    if (originalText == null || originalText.trim().isEmpty()) {
                        fallbackResult.add(Collections.nCopies(expectedDimension, 0.0));
                    } else {
                        if (currentBatchIdx < batchEmbeddings.size()) {
                            fallbackResult.add(batchEmbeddings.get(currentBatchIdx++));
                        } else {
                            log.error("Index out of bounds during fallback reconstruction");
                            fallbackResult.add(Collections.nCopies(expectedDimension, 0.0));
                        }
                    }
                }
                return fallbackResult;
            }

            List<List<Double>> finalResult = new ArrayList<>(texts.size());
            int batchResultIndex = 0;
            for (String originalText : texts) {
                if (originalText == null || originalText.trim().isEmpty()) {
                    finalResult.add(Collections.nCopies(expectedDimension, 0.0));
                } else {
                    finalResult.add(batchEmbeddings.get(batchResultIndex++));
                }
            }

            log.trace("Successfully processed batch embeddings for {} initial texts.", texts.size());
            return finalResult;

        } catch (Exception e) {
            log.error("Error calling Spring AI OpenAiEmbeddingModel batch embed: {}", e.getMessage(), e);
            return texts.stream().map(t -> Collections.nCopies(expectedDimension, 0.0)).collect(Collectors.toList());
        }
    }
}