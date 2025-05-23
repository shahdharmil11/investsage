package com.dharmil.investsage.dto;

import lombok.Data; // Or use getters/setters manually

@Data // Lombok annotation for getters, setters, toString, etc.
public class ChatRequest {
    private String message;
    // Add other fields if needed, e.g., userId, conversationId
}