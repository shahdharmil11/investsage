package com.dharmil.investsage.controller;

import com.dharmil.investsage.dto.ChatRequest;
import com.dharmil.investsage.dto.ChatResponse;
import com.dharmil.investsage.service.OpenAIService;
import com.dharmil.investsage.service.RagDataProcessor; // Import RagDataProcessor
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/chat") // Base path for chat endpoints
public class ChatController {

    private final OpenAIService openAIService;
    private final RagDataProcessor ragDataProcessor; // Inject RagDataProcessor

    // Use constructor injection (recommended)
    @Autowired
    public ChatController(OpenAIService openAIService, RagDataProcessor ragDataProcessor) {
        this.openAIService = openAIService;
        this.ragDataProcessor = ragDataProcessor;
    }

    @PostMapping
    public ResponseEntity<ChatResponse> handleChatMessage(@RequestBody ChatRequest chatRequest) {
        if (chatRequest == null || chatRequest.getMessage() == null || chatRequest.getMessage().trim().isEmpty()) {
            return ResponseEntity.badRequest().body(new ChatResponse("Message cannot be empty."));
        }

        try {
            // 1. Retrieve relevant chunks
            List<String> relevantChunks = ragDataProcessor.findSimilarChunks(chatRequest.getMessage(), 3); // Adjust limit as needed

            // 2. Construct the augmented prompt
            String augmentedPrompt = buildAugmentedPrompt(chatRequest.getMessage(), relevantChunks);

            // 3. Get the AI's response
            String reply = openAIService.getChatResponse(augmentedPrompt);

            return ResponseEntity.ok(new ChatResponse(reply));
        } catch (Exception e) {
            // Log the error server-side
            System.err.println("Error processing chat request: " + e.getMessage());
            // Provide a generic error response to the client
            return ResponseEntity.internalServerError().body(new ChatResponse("An internal error occurred. Please try again later."));
        }
    }

    private String buildAugmentedPrompt(String userMessage, List<String> contextChunks) {
        String contextSection = contextChunks.stream().map(chunk -> "--- " + chunk + " ---").collect(Collectors.joining("\n"));
        return "Context:\n" + contextSection + "\n\nQuestion:\n" + userMessage +
                "\n\nInstruction: Use the context to answer the question. If the answer is not in the context, say \"I don't know. No data is provided to us\"";
    }
}