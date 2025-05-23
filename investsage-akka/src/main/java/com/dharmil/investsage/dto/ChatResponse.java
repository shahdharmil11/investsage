package com.dharmil.investsage.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data // Lombok
@NoArgsConstructor // Lombok
@AllArgsConstructor // Lombok
public class ChatResponse {
    private String reply;
    // Add other fields if needed, e.g., sources, errors
}