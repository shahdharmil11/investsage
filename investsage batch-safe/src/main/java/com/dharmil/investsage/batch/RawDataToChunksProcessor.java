package com.dharmil.investsage.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RawDataToChunksProcessor implements ItemProcessor<RawDataRecord, List<ChunkInfo>> {

    private static final Logger log = LoggerFactory.getLogger(RawDataToChunksProcessor.class);
    // Target size for chunks - we'll try to group sentences close to this size.
    private static final int TARGET_CHUNK_SIZE = 500;
    // Optional: Minimum chunk size to avoid very small chunks unless necessary
    private static final int MIN_CHUNK_SIZE = 50;
    // Regex to split text into sentences (handles ., ?, ! followed by space or end)
    // Adjust if needed for more complex sentence boundary detection (e.g., abbreviations)
    private static final Pattern SENTENCE_PATTERN = Pattern.compile("[^.!?]+[.!?]+\\s*|[^.!?]+$");


    @Override
    @Nullable
    public List<ChunkInfo> process(@NonNull RawDataRecord item) throws Exception {
        List<ChunkInfo> chunkInfos = new ArrayList<>();
        String rawText = item.getRawText();
        int rawId = item.getId();

        if (rawText == null || rawText.trim().isEmpty()) {
            log.warn("Skipping empty raw text for raw_id: {}", rawId);
            return null; // Skip this item entirely if text is empty
        }

        // 1. Split text into sentences
        List<String> sentences = splitIntoSentences(rawText.trim());
        if (sentences.isEmpty()) {
            log.warn("Could not split text into sentences for raw_id: {}", rawId);
            // Optionally, create one chunk with the whole text if splitting fails
            // chunkInfos.add(new ChunkInfo(rawId, 0, rawText.trim()));
            // return chunkInfos;
            return null;
        }

        // 2. Group sentences into chunks
        List<String> textChunks = groupSentencesIntoChunks(sentences);
        log.trace("Processing raw_id: {}, generated {} chunks from {} sentences.", rawId, textChunks.size(), sentences.size());

        // 3. Create ChunkInfo objects
        for (int i = 0; i < textChunks.size(); i++) {
            String chunk = textChunks.get(i);
            if (!chunk.trim().isEmpty()) {
                chunkInfos.add(new ChunkInfo(rawId, i, chunk));
            }
        }

        return chunkInfos.isEmpty() ? null : chunkInfos; // Return null if no valid chunks generated
    }

    /**
     * Splits the input text into sentences based on the SENTENCE_PATTERN.
     * @param text The text to split.
     * @return A list of sentences.
     */
    private List<String> splitIntoSentences(String text) {
        List<String> sentences = new ArrayList<>();
        Matcher matcher = SENTENCE_PATTERN.matcher(text);
        while (matcher.find()) {
            String sentence = matcher.group().trim();
            if (!sentence.isEmpty()) {
                sentences.add(sentence);
            }
        }
        return sentences;
    }

    /**
     * Groups sentences into chunks, trying to adhere to TARGET_CHUNK_SIZE.
     * @param sentences List of sentences.
     * @return List of text chunks.
     */
    private List<String> groupSentencesIntoChunks(List<String> sentences) {
        List<String> chunks = new ArrayList<>();
        StringBuilder currentChunk = new StringBuilder();
        int sentenceIndex = 0;

        while (sentenceIndex < sentences.size()) {
            String sentence = sentences.get(sentenceIndex);

            // Check if adding the next sentence would exceed the target size significantly,
            // but only if the current chunk is already reasonably large (>= MIN_CHUNK_SIZE).
            if (currentChunk.length() >= MIN_CHUNK_SIZE &&
                    currentChunk.length() + sentence.length() > TARGET_CHUNK_SIZE)
            {
                // Finalize the current chunk
                chunks.add(currentChunk.toString().trim());
                currentChunk = new StringBuilder(); // Start a new chunk
            }

            // Add the sentence to the current chunk (append space if needed)
            if (currentChunk.length() > 0) {
                currentChunk.append(" ");
            }
            currentChunk.append(sentence);
            sentenceIndex++;

            // Handle cases where a single sentence is already larger than the target size
            if (currentChunk.length() > TARGET_CHUNK_SIZE && chunks.isEmpty() && sentenceIndex == 1) {
                // If the very first sentence is too long, make it its own chunk
                chunks.add(currentChunk.toString().trim());
                currentChunk = new StringBuilder();
            }
        }

        // Add the last remaining chunk if it's not empty
        if (currentChunk.length() > 0) {
            chunks.add(currentChunk.toString().trim());
        }

        return chunks;
    }
}