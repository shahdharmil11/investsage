//package com.dharmil.investsage;
//
//import com.dharmil.investsage.service.OpenAiEmbeddingService;
//import com.dharmil.investsage.service.RagDataProcessor;
//
//public class RagTest {
//    public static void main(String[] args) {
//        // --- Setup ---
//        // !!! IMPORTANT: Ensure OPENAI_API_KEY is set as an environment variable
//        // or retrieved securely from your config !!!
//        String apiKey = System.getenv("OPENAI_API_KEY");
//        if (apiKey == null || apiKey.trim().isEmpty()) {
//            System.err.println("Error: OPENAI_API_KEY environment variable not set.");
//            return;
//        }
//        OpenAiEmbeddingService embeddingService = new OpenAiEmbeddingService(apiKey);
//        RagDataProcessor processor = new RagDataProcessor(embeddingService);
//
//        // --- Test Similarity Search ---
//        String testQuery = "What are the risks of investing in tech stocks?"; // Example query
//        int resultLimit = 5; // Get top 5 results
//
//        System.out.println("\n--- Testing Similarity Search ---");
//        System.out.println("Query: " + testQuery);
//        List<String> similarChunks = processor.findSimilarChunks(testQuery, resultLimit);
//
//        if (similarChunks.isEmpty()) {
//            System.out.println("No similar chunks found.");
//        } else {
//            System.out.println("Found " + similarChunks.size() + " relevant chunks:");
//            int i = 1;
//            for (String chunk : similarChunks) {
//                System.out.println("\n--- Chunk " + (i++) + " ---");
//                System.out.println(chunk);
//            }
//        }
//        // --- Optional: Trigger full processing if needed ---
//        // System.out.println("\n--- Starting Data Processing (if needed) ---");
//        // processor.processAndStoreEmbeddings(); // Be careful running this repeatedly
//    }
//}