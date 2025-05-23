

-- 1. Enable the pgvector extension (should already be installed via the Docker image)
CREATE EXTENSION IF NOT EXISTS vector;

-- 2. Create a table to store investment data and embeddings
--    Adjust dimensions (e.g., 384, 768, 1536) based on the embedding model you'll use in MYA-5.
--    Let's start with 384 as a common dimension for smaller models.
CREATE TABLE IF NOT EXISTS investment_data (
    id SERIAL PRIMARY KEY,               -- Unique ID for each data chunk
    content TEXT NOT NULL,               -- The original text content from the CSV
    source VARCHAR(255),                 -- Optional: Origin file or section
    embedding VECTOR(384)                -- The vector embedding (ADJUST DIMENSION LATER if needed)
);

-- 3. Optional: Create an index for faster vector similarity searches (Highly recommended for performance)
--    Using HNSW index (Hierarchical Navigable Small World) - generally good balance
--    Adjust 'm' and 'ef_construction' based on data size/performance needs
CREATE INDEX IF NOT EXISTS idx_investment_embedding_hnsw
ON investment_data
USING hnsw (embedding vector_cosine_ops) -- Use cosine distance for similarity
WITH (m = 16, ef_construction = 64);

-- Or use IVFFlat index (Inverted File Flat) - good for large datasets, may require tuning
-- CREATE INDEX IF NOT EXISTS idx_investment_embedding_ivfflat
-- ON investment_data
-- USING ivfflat (embedding vector_cosine_ops)
-- WITH (lists = 100); -- Adjust 'lists' based on dataset size (e.g., sqrt(N))















CREATE EXTENSION IF NOT EXISTS vector; -- Ensure pgvector is enabled

CREATE TABLE IF NOT EXISTS investment_embeddings (
                                                     id SERIAL PRIMARY KEY,
                                                     chunk_text TEXT NOT NULL,
                                                     embedding vector(1536) -- Dimension for text-embedding-3-small
    );

-- Optional: Create an index for faster similarity search
-- Using HNSW index (good balance of speed/accuracy), adjust parameters as needed
CREATE INDEX IF NOT EXISTS hnsw_investment_embedding_idx
    ON investment_embeddings
    USING hnsw (embedding vector_cosine_ops);

-- Or using IVFFlat index (might need tuning)
-- CREATE INDEX IF NOT EXISTS ivfflat_investment_embedding_idx
-- ON investment_embeddings
-- USING ivfflat (embedding vector_cosine_ops)
-- WITH (lists = 100); -- Adjust 'lists' based on dataset size















-- Ensure the pgvector extension is enabled (if not already)
-- CREATE EXTENSION IF NOT EXISTS vector;

-- Drop table if it exists (optional, useful during testing)
-- DROP TABLE IF EXISTS new_investment_embeddings;

-- Create the new table with metadata columns
CREATE TABLE IF NOT EXISTS new_investment_embeddings (
                                                         id SERIAL PRIMARY KEY,
                                                         raw_data_id INT,                     -- Foreign key reference to raw_investment_data
                                                         chunk_index INT NOT NULL,            -- Order of the chunk within the original text
                                                         chunk_text TEXT,
                                                         embedding vector(1536),              -- Adjust dimension if needed (1536 for text-embedding-3-small)
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP -- Optional timestamp
                             );

-- Create an index for efficient similarity search (HNSW is generally recommended)
-- Adjust parameters based on your data size and performance needs
CREATE INDEX IF NOT EXISTS idx_hnsw_new_embedding ON new_investment_embeddings
    USING hnsw (embedding vector_cosine_ops) -- Use vector_cosine_ops for OpenAI embeddings
    WITH (m = 16, ef_construction = 64);

-- Optional: Index for filtering by raw_data_id if needed often
CREATE INDEX IF NOT EXISTS idx_new_embedding_raw_data_id ON new_investment_embeddings (raw_data_id);





-- Grant usage on the public schema (where tables are usually created)
GRANT USAGE ON SCHEMA public TO investsage_user;

-- Grant all privileges on tables within the public schema to the user
-- This includes CREATE, SELECT, INSERT, UPDATE, DELETE etc. for tables created now or in the future by this user.
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO investsage_user;

-- Explicitly grant create on the schema as well
GRANT CREATE ON SCHEMA public TO investsage_user;

-- Optionally, grant privileges on sequences (needed for SERIAL columns used by Batch)
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO investsage_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO investsage_user; -- For existing sequences if any












-- SQL Script for Spring Batch Metadata Tables (PostgreSQL - Lowercase)
-- Version: Based on Spring Batch v5 schema
-- Purpose: Manual creation of tables for Spring Batch without Flyway.

-- Drop existing tables and sequences if they exist (optional, uncomment if needed for a clean setup)
/*
DROP TABLE IF EXISTS batch_step_execution_context CASCADE;
DROP TABLE IF EXISTS batch_job_execution_context CASCADE;
DROP TABLE IF EXISTS batch_step_execution CASCADE;
DROP TABLE IF EXISTS batch_job_execution_params CASCADE;
DROP TABLE IF EXISTS batch_job_execution CASCADE;
DROP TABLE IF EXISTS batch_job_instance CASCADE;

DROP SEQUENCE IF EXISTS batch_step_execution_seq;
DROP SEQUENCE IF EXISTS batch_job_execution_seq;
DROP SEQUENCE IF EXISTS batch_job_seq;
*/

-- Create sequences for generating primary keys (lowercase names)
-- Ensures sequences are created only if they don't already exist.
CREATE SEQUENCE IF NOT EXISTS batch_step_execution_seq MAXVALUE 9223372036854775807 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS batch_job_execution_seq MAXVALUE 9223372036854775807 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS batch_job_seq MAXVALUE 9223372036854775807 NO CYCLE;

-- Table: batch_job_instance (lowercase)
-- Stores information about job instances.
CREATE TABLE batch_job_instance (
                                    job_instance_id BIGINT NOT NULL PRIMARY KEY DEFAULT nextval('batch_job_seq'), -- Use sequence for default value
                                    version BIGINT,
                                    job_name VARCHAR(100) NOT NULL,
                                    job_key VARCHAR(32) NOT NULL,
                                    CONSTRAINT job_inst_un UNIQUE (job_name, job_key) -- Lowercase constraint name for uniqueness
);

-- Table: batch_job_execution (lowercase)
-- Stores information about individual job executions.
CREATE TABLE batch_job_execution (
                                     job_execution_id BIGINT NOT NULL PRIMARY KEY DEFAULT nextval('batch_job_execution_seq'), -- Use sequence
                                     version BIGINT,
                                     job_instance_id BIGINT NOT NULL,
                                     create_time TIMESTAMP(9) NOT NULL, -- Using TIMESTAMP(9) as per your example
                                     start_time TIMESTAMP(9) DEFAULT NULL,
                                     end_time TIMESTAMP(9) DEFAULT NULL,
                                     status VARCHAR(10),
                                     exit_code VARCHAR(2500),
                                     exit_message VARCHAR(2500),
                                     last_updated TIMESTAMP(9),
                                     job_configuration_location VARCHAR(2500) NULL,
                                     CONSTRAINT job_inst_exec_fk FOREIGN KEY (job_instance_id) -- Lowercase constraint name
                                         REFERENCES batch_job_instance(job_instance_id) -- References lowercase table/column
);

-- Table: batch_job_execution_params (lowercase)
-- Stores parameters used for a job execution. Schema updated for Spring Batch v5.
CREATE TABLE batch_job_execution_params (
                                            job_execution_id BIGINT NOT NULL,
                                            parameter_name VARCHAR(100) NOT NULL, -- Spring Batch v5 uses parameter_name
                                            parameter_type VARCHAR(100) NOT NULL, -- Spring Batch v5 uses parameter_type
                                            parameter_value VARCHAR(2500),        -- Spring Batch v5 uses parameter_value
                                            identifying CHAR(1) NOT NULL,         -- Flag indicating if the parameter identifies the job instance
                                            CONSTRAINT job_exec_params_fk FOREIGN KEY (job_execution_id) -- Lowercase constraint name
                                                REFERENCES batch_job_execution(job_execution_id) -- References lowercase table/column
    -- Note: Spring Batch v5 schema typically doesn't define a composite primary key here in the base script,
    -- but relies on the foreign key and application logic. Add if needed:
    -- , PRIMARY KEY (job_execution_id, parameter_name)
);

-- Table: batch_step_execution (lowercase)
-- Stores information about individual step executions within a job execution.
CREATE TABLE batch_step_execution (
                                      step_execution_id BIGINT NOT NULL PRIMARY KEY DEFAULT nextval('batch_step_execution_seq'), -- Use sequence
                                      version BIGINT NOT NULL,
                                      step_name VARCHAR(100) NOT NULL,
                                      job_execution_id BIGINT NOT NULL,
                                      create_time TIMESTAMP(9) NOT NULL,       -- Added in newer versions
                                      start_time TIMESTAMP(9) DEFAULT NULL,
                                      end_time TIMESTAMP(9) DEFAULT NULL,
                                      status VARCHAR(10),
                                      commit_count BIGINT,
                                      read_count BIGINT,
                                      filter_count BIGINT,
                                      write_count BIGINT,
                                      read_skip_count BIGINT,
                                      write_skip_count BIGINT,
                                      process_skip_count BIGINT,               -- Added this column
                                      rollback_count BIGINT,
                                      exit_code VARCHAR(2500),
                                      exit_message VARCHAR(2500),
                                      last_updated TIMESTAMP(9),               -- Added this column
                                      CONSTRAINT job_exec_step_fk FOREIGN KEY (job_execution_id) -- Lowercase constraint name
                                          REFERENCES batch_job_execution(job_execution_id) -- References lowercase table/column
);

-- Table: batch_step_execution_context (lowercase)
-- Stores the execution context for a step execution (serialized data).
CREATE TABLE batch_step_execution_context (
                                              step_execution_id BIGINT NOT NULL PRIMARY KEY,
                                              short_context VARCHAR(2500) NOT NULL,   -- Short representation of context
                                              serialized_context TEXT,               -- Full serialized context (use TEXT for potentially large data)
                                              CONSTRAINT step_exec_ctx_fk FOREIGN KEY (step_execution_id) -- Lowercase constraint name
                                                  REFERENCES batch_step_execution(step_execution_id) -- References lowercase table/column
);

-- Table: batch_job_execution_context (lowercase)
-- Stores the execution context for a job execution (serialized data).
CREATE TABLE batch_job_execution_context (
                                             job_execution_id BIGINT NOT NULL PRIMARY KEY,
                                             short_context VARCHAR(2500) NOT NULL,    -- Short representation of context
                                             serialized_context TEXT,                -- Full serialized context
                                             CONSTRAINT job_exec_ctx_fk FOREIGN KEY (job_execution_id) -- Lowercase constraint name
                                                 REFERENCES batch_job_execution(job_execution_id) -- References lowercase table/column
);

-- Optional: Add indexes for performance, especially on foreign keys and frequently queried columns.
-- Example indexes (uncomment and adjust as needed):
/*
CREATE INDEX idx_job_inst_name_key ON batch_job_instance(job_name, job_key);
CREATE INDEX idx_job_exec_inst ON batch_job_execution(job_instance_id);
CREATE INDEX idx_step_exec_job_exec ON batch_step_execution(job_execution_id);
*/

-- Script End














TRUNCATE TABLE
    batch_step_execution_context,
    batch_job_execution_context,
    batch_job_execution_params,
    batch_step_execution,
    batch_job_execution,
    batch_job_instance,
    new_investment_embeddings
    RESTART IDENTITY;