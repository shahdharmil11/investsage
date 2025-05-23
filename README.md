# InvestSage

InvestSage is an intelligent investment advisory system that leverages Akka for distributed processing and OpenAI's language models to provide personalized investment recommendations. The system processes large amounts of investment data efficiently using actor-based concurrent processing.

## Project Structure

The project consists of two main implementations:
1. **Batch Processing Version** (`investsage batch-safe/`)
2. **Akka-based Distributed Version** (`investsage-akka/`)

### Key Features

- Distributed data processing using Akka actors
- OpenAI integration for intelligent responses
- RAG (Retrieval-Augmented Generation) implementation
- Real-time chat interface for investment queries
- Efficient text chunking and embedding generation
- PostgreSQL database integration

### Architecture Components

#### Akka Implementation
- **Actor System**
  - ChunkerActor: Handles text chunking
  - DataReaderActor: Manages data reading operations
  - DbWriterActor: Handles database operations
  - EmbeddingActor: Processes embeddings using OpenAI
  - JobCoordinatorActor: Coordinates the overall workflow

#### Services
- OpenAI Integration
- Embedding Generation
- RAG Data Processing
- Investment Query Processing

## Tech Stack

- Java Spring Boot
- Akka Framework
- OpenAI API
- PostgreSQL
- Docker
- React (Frontend)

## Getting Started

### Prerequisites
- Java 11 or higher
- Maven
- Docker and Docker Compose
- PostgreSQL
- OpenAI API key

### Setup Instructions

1. Clone the repository:
   ```bash
   git clone git@github.com:shahdharmil11/investsage.git
   ```

2. Configure environment variables:
   - Set up OpenAI API key
   - Configure database credentials

3. Start the database using Docker:
   ```bash
   cd investsage-akka/src/main/java/com/dharmil/investsage/docker
   docker-compose up -d
   ```

4. Build the project:
   ```bash
   mvn clean install
   ```

5. Run the application:
   ```bash
   mvn spring-boot:run
   ```

## Data Processing Flow

1. Raw investment data is read in batches
2. Text is chunked into manageable segments
3. Chunks are processed for embeddings using OpenAI
4. Embeddings are stored in the database
5. User queries are processed using RAG for accurate responses

## API Endpoints

- `/api/chat`: Chat interface for investment queries
- `/api/investment`: Investment-specific query endpoint

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
