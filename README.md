# InvestSage AI: Intelligent Investment Advisory Chatbot

InvestSage AI is a full-stack application designed to provide intelligent investment advice. It leverages a powerful Retrieval-Augmented Generation (RAG) pipeline on the backend, built with Spring Boot and Akka actors, to provide contextual answers from your investment data. The frontend is a modern Next.js application, providing a user-friendly chat interface.

## 🚀 Features

* **Agentic AI Core:** Utilizes Spring AI for integration with OpenAI models (like `gpt-4.1-nano-2025-04-14` for chat and `text-embedding-3-small` for embeddings) to power the AI chatbot.
* **Scalable RAG Pipeline:** Implements a robust RAG process using Akka actors for efficient, parallel processing of data:
    * **Data Reading:** Reads raw investment data from a PostgreSQL database.
    * **Text Chunking:** Breaks down large texts into smaller, manageable chunks for embedding.
    * **Embedding Generation:** Creates vector embeddings for text chunks using OpenAI's embedding model.
    * **Vector Storage:** Stores embeddings in a PostgreSQL database with the `pgvector` extension for similarity search.
* **Contextual Responses:** Augments AI prompts with relevant information retrieved from your stored data, ensuring accurate and context-aware investment advice.
* **RESTful API:** Provides a clean API for chat interactions and direct chunk querying.
* **Modern Frontend:** A responsive and interactive chat interface built with Next.js, Tailwind CSS, and Shadcn UI components.
* **Dockerized Database:** Easy setup of the PostgreSQL database with `pgvector` using Docker Compose.

## ⚙️ Technologies Used

### Backend (Java/Spring Boot)

* **Spring Boot:** Framework for building the backend application.
* **Spring AI:** Integration with OpenAI's chat and embedding models.
* **Akka:** Actor system for building concurrent, distributed, and fault-tolerant RAG data processing pipeline.
* **PostgreSQL:** Relational database for storing raw data and vector embeddings.
* **PGVector:** PostgreSQL extension for efficient vector similarity search.
* **Maven:** Build automation tool.
* **Lombok:** Java library to reduce boilerplate code.

### Frontend (Next.js)

* **Next.js:** React framework for building server-side rendered and static web applications.
* **React:** JavaScript library for building user interfaces.
* **Tailwind CSS:** Utility-first CSS framework for rapid UI development.
* **Shadcn UI:** Reusable UI components built with Tailwind CSS and Radix UI.

## 🏛️ Architecture Overview

The backend is structured around a **Retrieval-Augmented Generation (RAG)** pattern.

1.  **Data Ingestion & Embedding (Akka Pipeline):**
    * **`DataUploader.java`**: A utility to load initial data from a CSV file into the `raw_investment_data` table in PostgreSQL.
    * **Akka Actors:** The core RAG pipeline is orchestrated by a network of Akka actors:
        * **`JobCoordinatorActor`**: Initiates and manages the entire embedding job, distributing tasks to readers and coordinating shutdowns of downstream actors.
        * **`DataReaderActor`**: Reads batches of raw text data from the `raw_investment_data` table.
        * **`ChunkerActor`**: Splits raw text into smaller, meaningful chunks based on configured sizes.
        * **`EmbeddingActor`**: Takes text chunks and sends them to the `OpenAiEmbeddingService` to generate vector embeddings.
        * **`DbWriterActor`**: Stores the processed chunks along with their vector embeddings into the `new_investment_embeddings` table in PostgreSQL.
    * This pipeline runs typically once (or on data updates) to pre-process and store investment data in a vector-searchable format.

2.  **Query & Chat (REST API & RAG):**
    * **`InvestmentQueryController`**: Exposes a REST endpoint (`/api/v1/investment/query`) to directly query for similar chunks based on a given text query and limit.
    * **`ChatController`**: Provides the main chat interface endpoint (`/api/chat`). When a user sends a message:
        1.  It uses `RagDataProcessor` to convert the user's query into an embedding.
        2.  It performs a similarity search on the `new_investment_embeddings` table to retrieve the most relevant chunks of information.
        3.  These relevant chunks are then used to augment the user's original message, forming a more informed prompt for the `OpenAIService`.
        4.  The `OpenAIService` interacts with the OpenAI Chat API (e.g., GPT-4o-mini) to generate a coherent response based on the augmented prompt.
    * **`RagDataProcessor`**: Manages the interaction with the `OpenAiEmbeddingService` for embedding queries and performs the PostgreSQL `pgvector` similarity search.

3.  **Frontend:**
    * The Next.js application (`investsage-frontend`) provides the user interface.
    * It interacts with the backend's `/api/chat` endpoint to send user messages and display AI responses.
    * The `next.config.ts` file is configured to proxy API requests from `/api/*` to the backend running on `http://localhost:8080`.

## 🛠️ Setup and Running Locally

### Prerequisites

* Java 17 or higher
* Maven
* Node.js (LTS version recommended)
* npm or Yarn
* Docker & Docker Compose (for PostgreSQL setup)

### 1. Database Setup (PostgreSQL with pgvector)

First, set up your PostgreSQL database using Docker Compose.

```bash
cd investsage-copy/src/main/java/com/dharmil/investsage/docker
docker-compose up -d