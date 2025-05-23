package com.dharmil.investsage.service;

// --- Spring AI Imports ONLY ---

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.messages.Message;
import org.springframework.ai.chat.messages.SystemMessage;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
public class OpenAIService {

    private static final Logger log = LoggerFactory.getLogger(OpenAIService.class);

    private final ChatClient chatClient;

    public OpenAIService(ChatClient.Builder chatClientBuilder) {
        this.chatClient = chatClientBuilder.build();
        log.info("Spring AI ChatClient (for OpenAI) initialized via Builder.");
    }

    public String getChatResponse(String userMessage) {
        log.debug("Creating chat prompt for user message...");

        SystemMessage systemMessage = new SystemMessage("You are a helpful investment advisor assistant.");
        UserMessage userReqMessage = new UserMessage(userMessage);
        List<Message> messageList = List.of(systemMessage, userReqMessage);
        Prompt prompt = new Prompt(messageList);

        log.info("Sending prompt using Spring AI ChatClient (attempting M6 structure)...");
        try {
            // 3. CALL METHOD: Try the fluent API style which became standard.
            // This builds the request specification and then executes it.
            ChatResponse response = chatClient.prompt(prompt) // Start building with the prompt
                    .call()          // Execute the call
                    .chatResponse(); // Get the ChatResponse object

            log.info("Received response from ChatClient.");

            // 4. EXTRACT CONTENT: Try common M6 variations for response structure.

            // POSSIBILITY A: Response contains a list of results/generations (Common pattern).
            String content = Optional.ofNullable(response)
                    .map(ChatResponse::getResults) // <- Try getResults() (plural) first
                    .filter(results -> !results.isEmpty())
                    .map(results -> results.get(0)) // Get the first Generation/Result object
                    .map(generation -> generation.getOutput().getText()) // Get content from Generation's output message
                    .orElse(null);

            // POSSIBILITY B: If getResults() didn't work or returned null/empty, try getResult() (singular)
            if (content == null) {
                log.warn("Could not extract content using getResults(). Trying getResult() (singular)...");
                content = Optional.ofNullable(response)
                        .map(ChatResponse::getResult) // <- Fallback to getResult() (singular)
                        .map(result -> result.getOutput().getText()) // Get content from the single result's output
                        .orElse(null);
            }

            if (content != null) {
                log.debug("Successfully extracted content from ChatResponse using M6 logic attempt.");
                return content;
            } else {
                // Log the raw response object might help debug its structure if needed
                // Be cautious logging full responses if they contain sensitive data.
                log.warn("Received ChatResponse, but failed to extract content using known M6 patterns. Response: {}", response);
                return "Sorry, I received an unexpected response format from the AI service (M6).";
            }

        } catch (Exception e) {
            log.error("Error interacting with AI Service via Spring AI ChatClient (M6): {}", e.getMessage(), e);
            // Check exception type - Spring AI might throw specific exceptions for API errors.
            return "Sorry, I encountered an error communicating with the AI service via Spring AI (M6).";
        }
    }
}