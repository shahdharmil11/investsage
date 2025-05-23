package com.dharmil.investsage.akka.actor;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.dharmil.investsage.akka.di.SpringAkkaExtension;
// Removed ChunkProcessedAck import
import com.dharmil.investsage.akka.messages.ChunkToEmbed;
import com.dharmil.investsage.akka.messages.EmbeddingResult;
import com.dharmil.investsage.model.ChunkInfo;
import com.dharmil.investsage.model.EmbeddingRecord;
import com.dharmil.investsage.service.OpenAiEmbeddingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Component("embeddingActor")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class EmbeddingActor extends AbstractActor {

    private static final Logger log = LoggerFactory.getLogger(EmbeddingActor.class);

    // --- Dependencies ---
    @Autowired
    private OpenAiEmbeddingService embeddingService;
    private final ActorRef dbWriterRouterRef;
    // --- REMOVED chunkerActorRef ---

    // --- State (Reverted counters) ---
    private final AtomicLong chunksReceived = new AtomicLong(0);
    private final AtomicLong embeddingsAttempted = new AtomicLong(0);
    private final AtomicLong embeddingsSucceeded = new AtomicLong(0);
    private final AtomicLong embeddingsFailed = new AtomicLong(0);
    private final AtomicLong resultsSent = new AtomicLong(0);
    // Removed acksSent counter
    private String actorInstanceId;

    // --- Original Props Factory ---
    public static Props props(ActorSystem system, ActorRef dbWriterRouterRef) {
        return SpringAkkaExtension.provider.get(system).props("embeddingActor", dbWriterRouterRef);
    }

    // --- Original Constructor ---
    @Autowired(required = false)
    public EmbeddingActor(ActorRef dbWriterRouterRef) {
        this.dbWriterRouterRef = dbWriterRouterRef;
        this.actorInstanceId = getSelf().path().name();
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        log.info("EmbeddingActor instance [{}] started.", actorInstanceId);
        if (embeddingService == null) {
            log.error("[{}] OpenAiEmbeddingService not injected correctly!", actorInstanceId);
            throw new IllegalStateException("Required dependency OpenAiEmbeddingService not injected by Spring.");
        }
        if (dbWriterRouterRef == null) {
            log.error("[{}] dbWriterRouterRef was not provided during actor creation!", actorInstanceId);
            throw new IllegalStateException("Required argument dbWriterRouterRef is null.");
        }
        // Removed chunkerActorRef check
    }

    @Override
    public void postStop() throws Exception {
        // Original postStop log
        log.info("EmbeddingActor instance [{}] stopping. Final Counts -> Received: {}, Attempted: {}, Succeeded: {}, Failed: {}, Results Sent: {}",
                actorInstanceId, chunksReceived.get(), embeddingsAttempted.get(), embeddingsSucceeded.get(), embeddingsFailed.get(), resultsSent.get());
        super.postStop();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ChunkToEmbed.class, this::handleChunkToEmbed)
                .matchAny(this::handleUnknownMessage)
                .build();
    }

    private void handleChunkToEmbed(ChunkToEmbed msg) {
        // Keep the ENTERED log for debugging if needed
        log.debug("[{}] handleChunkToEmbed ENTERED for rawId {}, chunkIndex {}.",
                actorInstanceId, msg.chunkInfo().getRawDataId(), msg.chunkInfo().getChunkIndex());

        long receivedCount = chunksReceived.incrementAndGet();
        ChunkInfo chunkInfo = msg.chunkInfo();
        log.debug("[{}] Processing Received Chunk #{}: rawId {}, chunkIndex {}. Text length: {}",
                actorInstanceId, receivedCount, chunkInfo.getRawDataId(), chunkInfo.getChunkIndex(), chunkInfo.getText().length());

        if (embeddingService == null) {
            embeddingsFailed.incrementAndGet(); // Count failure
            throw new IllegalStateException("EmbeddingService is null for actor [" + actorInstanceId +"] processing chunk rawId " + chunkInfo.getRawDataId());
        }

        long attemptNumber = embeddingsAttempted.incrementAndGet();
        boolean processingSucceeded = false; // Still useful for internal logic

        // --- REMOVED finally block for ACK ---
        try {
            log.debug("[{}] Attempting embedding #{} for chunk rawId {}, chunkIndex {}...",
                    actorInstanceId, attemptNumber, chunkInfo.getRawDataId(), chunkInfo.getChunkIndex());

            // --- Call the service ---
            List<Double> embeddingDoubleList = embeddingService.getEmbedding(chunkInfo.getText());

            // --- Log raw service result BEFORE validation ---
            log.debug("[{}] Service returned list size: {} for rawId {}, chunkIndex {}",
                    actorInstanceId, (embeddingDoubleList == null ? "null" : embeddingDoubleList.size()),
                    chunkInfo.getRawDataId(), chunkInfo.getChunkIndex());

            // --- Validate Result (throws exception on failure) ---
            if (embeddingDoubleList == null || embeddingDoubleList.isEmpty() || embeddingDoubleList.stream().allMatch(d -> d == 0.0)) {
                // Count failure before throwing
                embeddingsFailed.incrementAndGet();
                throw new RuntimeException("Embedding service returned empty or zero vector.");
            }

            float[] embeddingFloatArray = convertToFloatArray(embeddingDoubleList); // Can return null
            if (embeddingFloatArray == null) {
                // Count failure before throwing
                embeddingsFailed.incrementAndGet();
                throw new RuntimeException("Failed to convert embedding List<Double> to float[].");
            }

            // --- Success Path ---
            embeddingsSucceeded.incrementAndGet();
            log.info("[{}] Embedding #{} SUCCEEDED for rawId {}, chunkIndex {}. Dimension: {}.", // Removed API time as service logs it
                    actorInstanceId, attemptNumber, chunkInfo.getRawDataId(), chunkInfo.getChunkIndex(), embeddingFloatArray.length);

            EmbeddingRecord record = new EmbeddingRecord(
                    chunkInfo.getRawDataId(), chunkInfo.getChunkIndex(), chunkInfo.getText(), embeddingFloatArray
            );
            EmbeddingResult resultMsg = new EmbeddingResult(record);

            // Send downstream
            dbWriterRouterRef.tell(resultMsg, getSelf());
            resultsSent.incrementAndGet();
            processingSucceeded = true; // Mark that result was sent
            log.debug("[{}] Sent EmbeddingResult #{} for rawId {}, chunkIndex {} downstream.",
                    actorInstanceId, resultsSent.get(), chunkInfo.getRawDataId(), chunkInfo.getChunkIndex());

        } catch (Exception e) {
            // Count failure only if we didn't succeed before exception
            if (!processingSucceeded) {
                embeddingsFailed.incrementAndGet();
            }
            String errorMsg = String.format("[%s] Embedding attempt #%d FAILED for rawId %d, chunkIndex %d.",
                    actorInstanceId, attemptNumber, chunkInfo.getRawDataId(), chunkInfo.getChunkIndex());
            log.error(errorMsg, e); // Log the full exception

            // Re-throw to let supervisor handle actor stop
            if (e instanceof RuntimeException) { throw (RuntimeException) e; }
            else { throw new RuntimeException(errorMsg, e); } // Wrap non-runtime exceptions
        }
        // --- ACK sending removed ---
    }

    private void handleUnknownMessage(Object msg) {
        log.warn("[{}] Received unknown message: {} from {}", actorInstanceId, msg.getClass().getName(), getSender());
    }

    private float[] convertToFloatArray(List<Double> list) {
        if (list == null) return null;
        float[] array = new float[list.size()];
        for (int i = 0; i < list.size(); i++) {
            Double d = list.get(i);
            if (d == null) {
                log.error("[{}] Null value found in embedding list at index {}. Cannot convert.", actorInstanceId, i);
                return null; // Indicate failure
            }
            array[i] = d.floatValue();
        }
        return array;
    }
}