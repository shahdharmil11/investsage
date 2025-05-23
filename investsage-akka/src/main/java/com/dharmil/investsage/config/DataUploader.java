package com.dharmil.investsage.config;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class DataUploader {

    // --- Database Configuration ---
    // !!! REPLACE WITH YOUR ACTUAL DATABASE DETAILS !!!
    private static final String DB_URL = "jdbc:postgresql://localhost:5433/investsage_db";
    private static final String DB_USER = "investsage_user";
    private static final String DB_PASSWORD = "yoursecurepassword";
    private static final String STAGING_TABLE_NAME = "raw_investment_data"; // You can change this if needed

    // --- CSV File Path ---
    private static final String CSV_FILE_PATH = "/Users/dharmilshah/Downloads/akka/Cleaned_data for investment advice.csv";

    public static void main(String[] args) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        CSVReader reader = null;

        try {
            // 1. Establish Database Connection
            System.out.println("Connecting to database...");
            connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
            System.out.println("Database connection established.");

            // Disable auto-commit for batch insertion
            connection.setAutoCommit(false);

            // 2. Create Staging Table (if it doesn't exist)
            createStagingTable(connection);

            // 3. Prepare SQL Insert Statement
            String insertSql = "INSERT INTO " + STAGING_TABLE_NAME + " (raw_text) VALUES (?)";
            preparedStatement = connection.prepareStatement(insertSql);

            // 4. Read CSV and Insert Data
            System.out.println("Reading CSV file: " + CSV_FILE_PATH);
            reader = new CSVReader(new FileReader(CSV_FILE_PATH));
            String[] nextLine;
            int rowCount = 0;
            int batchSize = 100; // Insert in batches for efficiency
            String[] headers = reader.readNext(); // Read header row

            if (headers == null) {
                System.err.println("Error: CSV file is empty or header row is missing.");
                return;
            }

            // Find column indices (adjust if your column names differ)
            int instructionCol = -1, inputCol = -1, outputCol = -1;
            for(int i=0; i<headers.length; i++) {
                if ("instruction".equalsIgnoreCase(headers[i])) instructionCol = i;
                if ("input".equalsIgnoreCase(headers[i])) inputCol = i;
                if ("output".equalsIgnoreCase(headers[i])) outputCol = i;
            }

            if (instructionCol == -1 || inputCol == -1 || outputCol == -1) {
                System.err.println("Error: Could not find required columns ('instruction', 'input', 'output') in CSV header.");
                System.err.println("Found headers: " + String.join(", ", headers));
                return;
            }

            System.out.println("Starting data insertion...");
            while ((nextLine = reader.readNext()) != null) {
                rowCount++;
                // Concatenate relevant text columns
                String instruction = (instructionCol < nextLine.length && nextLine[instructionCol] != null) ? nextLine[instructionCol] : "";
                String input = (inputCol < nextLine.length && nextLine[inputCol] != null) ? nextLine[inputCol] : "";
                String output = (outputCol < nextLine.length && nextLine[outputCol] != null) ? nextLine[outputCol] : "";

                String combinedText = instruction.trim() + " " + input.trim() + " " + output.trim(); // Combine and trim
                combinedText = combinedText.trim(); // Trim again in case some parts were empty

                if (!combinedText.isEmpty()) {
                    preparedStatement.setString(1, combinedText);
                    preparedStatement.addBatch();
                } else {
                    System.out.println("Skipping empty combined text at row: " + rowCount);
                }


                // Execute batch insert periodically
                if (rowCount % batchSize == 0) {
                    System.out.println("Executing batch insert at row: " + rowCount);
                    preparedStatement.executeBatch();
                    connection.commit(); // Commit the transaction batch
                }
            }

            // Execute any remaining inserts in the last batch
            System.out.println("Executing final batch...");
            preparedStatement.executeBatch();
            connection.commit(); // Commit the final transaction

            System.out.println("Data insertion complete. Total rows processed: " + (rowCount)); // rowCount includes header

        } catch (SQLException e) {
            System.err.println("Database error occurred:");
            e.printStackTrace();
            // Attempt to rollback transaction on error
            if (connection != null) {
                try {
                    System.err.println("Transaction is being rolled back");
                    connection.rollback();
                } catch (SQLException excep) {
                    excep.printStackTrace();
                }
            }
        } catch (IOException e) {
            System.err.println("Error reading CSV file:");
            e.printStackTrace();
        } catch (CsvValidationException e) {
            System.err.println("Error validating CSV content:");
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("An unexpected error occurred:");
            e.printStackTrace();
        } finally {
            // 5. Close resources
            try {
                if (reader != null) reader.close();
                if (preparedStatement != null) preparedStatement.close();
                if (connection != null) {
                    connection.setAutoCommit(true); // Restore auto-commit
                    connection.close();
                    System.out.println("Database connection closed.");
                }
            } catch (SQLException | IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static void createStagingTable(Connection connection) throws SQLException {
        String createTableSQL = "CREATE TABLE IF NOT EXISTS " + STAGING_TABLE_NAME + " (" +
                "id SERIAL PRIMARY KEY, " +
                "raw_text TEXT" +
                ");";
        try (Statement statement = connection.createStatement()) {
            System.out.println("Executing: " + createTableSQL);
            statement.execute(createTableSQL);
            connection.commit(); // Commit table creation
            System.out.println("Table '" + STAGING_TABLE_NAME + "' checked/created successfully.");
        } catch (SQLException e) {
            System.err.println("Error creating staging table:");
            // Rollback if table creation fails
            connection.rollback();
            throw e; // Re-throw the exception
        }
    }
}