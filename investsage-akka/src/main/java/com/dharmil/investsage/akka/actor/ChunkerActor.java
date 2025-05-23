package com.dharmil.investsage.akka.actor;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.dharmil.investsage.akka.di.SpringAkkaExtension;
// Removed ChunkProcessedAck import
import com.dharmil.investsage.akka.messages.ChunkToEmbed;
import com.dharmil.investsage.akka.messages.RawDataBatch;
import com.dharmil.investsage.model.ChunkInfo;
import com.dharmil.investsage.model.RawDataRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
// Removed Queue and LinkedList imports
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component("chunkerActor")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ChunkerActor extends AbstractActor {

    private static final Logger log = LoggerFactory.getLogger(ChunkerActor.class);

    private final ChunkerConfiguration chunkerConfig;
    private final ActorRef embeddingRouterRef;

    // --- State (Reverted counters) ---
    private final AtomicLong totalBatchesReceived = new AtomicLong(0);
    private final AtomicLong totalRecordsReceived = new AtomicLong(0);
    private final AtomicLong totalChunksGenerated = new AtomicLong(0); // Keep track of generated
    private final AtomicLong totalChunksSent = new AtomicLong(0); // Back to simple sent counter

    // --- REMOVED: Buffering and Throttling State ---

    // --- Original Configuration Class ---
    public record ChunkerConfiguration(int targetChunkSize, int minChunkSize) {}

    private static final Pattern SENTENCE_PATTERN = Pattern.compile("[^.!?]+[.!?]+\\s*|[^.!?]+$");

    // Props factory takes original ChunkerConfiguration
    public static Props props(ActorSystem system, ChunkerConfiguration chunkerConfig, ActorRef embeddingRouterRef) {
        return SpringAkkaExtension.provider.get(system).props("chunkerActor", chunkerConfig, embeddingRouterRef);
    }

    // Constructor takes original ChunkerConfiguration
    @Autowired(required = false)
    public ChunkerActor(ChunkerConfiguration chunkerConfig, ActorRef embeddingRouterRef) {
        this.chunkerConfig = chunkerConfig;
        this.embeddingRouterRef = embeddingRouterRef;
        log.info("ChunkerActor instance created. Config: {}", chunkerConfig);
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        log.info("ChunkerActor instance started.");
        if (embeddingRouterRef == null) { throw new IllegalStateException("embeddingRouterRef is null"); }
    }

    @Override
    public void postStop() throws Exception {
        // Reverted postStop log
        log.info("ChunkerActor stopping attempt. Counts -> Batches Recv: {}, Records Recv: {}, Chunks Generated: {}, Chunks Sent: {}",
                totalBatchesReceived.get(), totalRecordsReceived.get(), totalChunksGenerated.get(), totalChunksSent.get());
        super.postStop();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(RawDataBatch.class, this::handleRawDataBatch)
                // Removed ACK handler
                .matchAny(this::handleUnknownMessage)
                .build();
    }

    // --- Original Batch Handler (Sends chunks directly) ---
    private void handleRawDataBatch(RawDataBatch msg) {
        long batchNumber = totalBatchesReceived.incrementAndGet();
        int recordsInBatch = msg.records().size();
        long currentTotalRecords = totalRecordsReceived.addAndGet(recordsInBatch);

        log.debug("ChunkerActor received Batch #{} with {} records. Total records received so far: {}",
                batchNumber, recordsInBatch, currentTotalRecords);

        int chunksGeneratedThisBatch = 0;
        int chunksSentThisBatch = 0;
        for (RawDataRecord record : msg.records()) {
            if (record.getRawText() == null || record.getRawText().trim().isEmpty()) {
                log.warn("ChunkerActor skipping record ID {} in Batch #{} due to empty raw text.", record.getId(), batchNumber);
                continue;
            }
            try {
                List<String> textChunks = chunkRawText(record.getRawText().trim());
                chunksGeneratedThisBatch += textChunks.size(); // Count generated

                for (int i = 0; i < textChunks.size(); i++) {
                    String chunkText = textChunks.get(i);
                    if (!chunkText.isEmpty()) {
                        ChunkInfo chunkInfo = new ChunkInfo(record.getId(), i, chunkText);
                        ChunkToEmbed chunkMsg = new ChunkToEmbed(chunkInfo);
                        // --- Send directly ---
                        log.debug("ChunkerActor telling EmbeddingRouter: Chunk rawId {}, chunkIndex {}", chunkInfo.getRawDataId(), chunkInfo.getChunkIndex());
                        embeddingRouterRef.tell(chunkMsg, getSelf());
                        chunksSentThisBatch++;
                        totalChunksSent.incrementAndGet(); // Increment overall sent count
                        // --- Removed buffering ---
                    }
                }
            } catch (Exception e) {
                log.error("ChunkerActor failed to chunk text for record ID {} in Batch #{}. Skipping record. Error: {}",
                        record.getId(), batchNumber, e.getMessage(), e);
            }
        }
        long currentTotalChunksGenerated = totalChunksGenerated.addAndGet(chunksGeneratedThisBatch);

        // --- Reverted Log ---
        log.info("ChunkerActor finished processing Batch #{}, generated {} chunks, sent {} chunks. Cumulative totals -> Records Recv: {}, Chunks Generated: {}, Chunks Sent: {}",
                batchNumber, chunksGeneratedThisBatch, chunksSentThisBatch, currentTotalRecords, currentTotalChunksGenerated, totalChunksSent.get());

        // --- Removed call to trySendBufferedChunks ---
    }

    private void handleUnknownMessage(Object msg) {
        log.warn("ChunkerActor received unknown message: {} from {}", msg.getClass().getName(), getSender());
    }

    // --- REMOVED trySendBufferedChunks ---

    // --- Helper Methods ---
    private List<String> chunkRawText(String rawText) {
        List<String> sentences = splitIntoSentences(rawText);
        if (sentences.isEmpty()) {
            log.warn("Could not split text into sentences. Treating whole text as one chunk (if <= target size). Text snippet: '{}'",
                    rawText.substring(0, Math.min(rawText.length(), 100)));
            if (rawText.length() <= chunkerConfig.targetChunkSize()) {
                return List.of(rawText);
            } else {
                log.error("Text could not be split into sentences and exceeds target chunk size ({} > {}). Skipping chunking for this text.",
                        rawText.length(), chunkerConfig.targetChunkSize());
                return List.of();
            }
        }
        return groupSentencesIntoChunks(sentences);
    }

    private List<String> splitIntoSentences(String text) {
        List<String> sentences = new ArrayList<>();
        if (text == null || text.trim().isEmpty()) { return sentences; }
        Matcher matcher = SENTENCE_PATTERN.matcher(text.trim());
        while (matcher.find()) {
            String sentence = matcher.group().trim();
            if (!sentence.isEmpty()) { sentences.add(sentence); }
        }
        return sentences;
    }

    private List<String> groupSentencesIntoChunks(List<String> sentences) {
        List<String> chunks = new ArrayList<>();
        StringBuilder currentChunk = new StringBuilder();
        int sentenceIndex = 0;
        while (sentenceIndex < sentences.size()) {
            String sentence = sentences.get(sentenceIndex);
            if (currentChunk.length() >= chunkerConfig.minChunkSize() &&
                    currentChunk.length() + sentence.length() + 1 > chunkerConfig.targetChunkSize()) {
                String chunkToAdd = currentChunk.toString().trim();
                if (!chunkToAdd.isEmpty()) { chunks.add(chunkToAdd); }
                currentChunk = new StringBuilder();
            }
            if (currentChunk.length() > 0) { currentChunk.append(" "); }
            currentChunk.append(sentence);
            sentenceIndex++;
            if (currentChunk.length() > chunkerConfig.targetChunkSize() && chunks.isEmpty() && currentChunk.toString().trim().equals(sentence)) {
                log.warn("Single sentence exceeds target chunk size ({} > {}). Creating oversized chunk.", currentChunk.length(), chunkerConfig.targetChunkSize());
                chunks.add(currentChunk.toString().trim());
                currentChunk = new StringBuilder();
            }
        }
        String lastChunk = currentChunk.toString().trim();
        if (!lastChunk.isEmpty()) { chunks.add(lastChunk); }
        return chunks;
    }
}