/*
 * Copyright 2019 Couchbase, Inc.
 */
package com.couchbase.loader;

import com.couchbase.client.core.time.Delay;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.util.retry.RetryBuilder;
import com.teradata.tpcds.Results;
import com.teradata.tpcds.Table;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import rx.Observable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class TpcdsGeneratorAndLoaderRunnable implements Runnable {

    private static final Logger LOGGER = LogManager.getRootLogger();

    // Partitioning members
    private int count;

    // Table members
    private final List<Table> selectedTables;
    private final List<Iterator<List<List<String>>>> tableIterators = new ArrayList<>();
    private Table currentTable;
    private final int tableCount;
    private int currentTableIndex;
    private final boolean generateAllTables;

    // Configuration
    private TpcdsConfiguration tpcdsConfiguration;
    private BucketUpsertConfiguration bucketUpsertConfiguration;

    // List to batch the insertion
    private ArrayList<JsonDocument> generatedJsonDocuments = new ArrayList<>();

    TpcdsGeneratorAndLoaderRunnable(TpcdsConfiguration tpcdsConfiguration,
            BucketUpsertConfiguration bucketUpsertConfiguration) {
        this.tpcdsConfiguration = tpcdsConfiguration;
        this.bucketUpsertConfiguration = bucketUpsertConfiguration;

        // If the tableName is null, then we're generating all the tables
        generateAllTables = tpcdsConfiguration.getTableToGenerate() == null;

        // Get the table(s)
        selectedTables = getTableFromStringTableName(tpcdsConfiguration.getTableToGenerate());

        // These variables will monitor and assist with each table's data generation
        currentTableIndex = 0;
        tableCount = selectedTables.size();
        currentTable = selectedTables.get(currentTableIndex);

        // Iterators for the tables to generate the data for
        for (Table table : selectedTables) {
            Results result = Results.constructResults(table, tpcdsConfiguration.getSession());
            tableIterators.add(result.iterator());
        }
    }

    @Override public void run() {
        boolean continueGeneration;

        // Loop until the conditions stop the loop
        while (true) {
            continueGeneration = false;

            // Current table still has more
            if (tableIterators.get(currentTableIndex).hasNext()) {
                continueGeneration = true;
            }
            // We went over all the tables
            else if (currentTableIndex == tableCount - 1) {
                break;
            }

            // Flag is true, generate the next one
            if (continueGeneration) {
                count++;
                generateAndUpsert(tableIterators.get(currentTableIndex).next());
            } else {
                // Go to the next table
                currentTableIndex++;
                currentTable = selectedTables.get(currentTableIndex);
            }
        }

        // Upsert any leftovers
        if (!generatedJsonDocuments.isEmpty()) {
            List<JsonDocument> copy = new ArrayList<>(generatedJsonDocuments);
            Observable.from(copy).flatMap(
                    (final JsonDocument docToInsert) -> bucketUpsertConfiguration.getBucket().async()
                            .upsert(docToInsert).retryWhen(RetryBuilder.anyOf(Exception.class).delay(Delay
                                    .fixed(bucketUpsertConfiguration.getFailureRetryDelay(), TimeUnit.MILLISECONDS))
                                    .max(bucketUpsertConfiguration.getFailureMaximumRetries()).build()))
                    .onErrorReturn(throwable -> {
                        System.out.println(throwable.getMessage());
                        return null;
                    }).toBlocking().last();
        }

        LOGGER.log(Level.INFO, "Partition " + tpcdsConfiguration.getPartition() + " generated " + count + " records");
        System.out.println("Partition " + tpcdsConfiguration.getPartition() + " generated " + count + " records");
    }

    /**
     * Gets the table matching the provided string table name, throws an exception if no table is returned.
     *
     * @param tableName String table name to search for.
     * @return Table if found, throws an exception otherwise.
     */
    private List<Table> getTableFromStringTableName(String tableName) {

        // Get all the tables
        if (generateAllTables) {
            // Remove the DBGEN_VERSION table and all children tables, parent tables will generate them
            return Table.getBaseTables().stream()
                    .filter(table -> !table.equals(Table.DBGEN_VERSION) && !table.isChild())
                    .collect(Collectors.toList());
        }

        // Search for the table
        List<Table> searchedTable =
                Table.getBaseTables().stream().filter(table -> tableName.equalsIgnoreCase(table.getName()))
                        .collect(Collectors.toList());

        if (searchedTable.isEmpty()) {
            throw new IllegalStateException("Invalid table name");
        }

        return searchedTable;
    }

    /**
     * Generates the TPC-DS records and feeds them to the KV bucket
     *
     * @param values List containing all the generated column values
     */
    private void generateAndUpsert(List<List<String>> values) {
        // increment the counter for parent record
        count++;

        // Create an empty JsonObject to fill in with the record data
        JsonObject parentRecord = JsonObject.empty();
        parentRecord.put("tableName", currentTable.toString());

        // Build the record data
        for (int counter = 0; counter < values.get(0).size(); counter++) {
            parentRecord.put(currentTable.getColumns()[counter].getName(), values.get(0).get(counter));
        }

        // JsonDocument with key and created record
        JsonDocument parentJsonDocument = JsonDocument
                .create(currentTable.getName() + "-" + ((tpcdsConfiguration.getPartition() - 1) + (count
                        * tpcdsConfiguration.getPartitions())), parentRecord);

        // Collecting records for batch upsert
        generatedJsonDocuments.add(parentJsonDocument);

        // In some cases, the generator generates 2 records, one for current table, and one for child table.
        if (generateAllTables && values.size() > 1) {

            // increment the counter for child record
            count++;

            // Create an empty JsonObject to fill in with the record data
            JsonObject childRecord = JsonObject.empty();
            childRecord.put("tableName", currentTable.getChild().toString());

            // Build the record data
            for (int counter = 0; counter < values.get(0).size(); counter++) {
                childRecord.put(currentTable.getColumns()[counter].getName(), values.get(0).get(counter));
            }

            // JsonDocument with key and created record
            JsonDocument childJsonDocument =
                    JsonDocument.create(currentTable.getChild().toString() + "-" + count, childRecord);

            // Collecting records for batch upsert
            generatedJsonDocuments.add(childJsonDocument);
        }

        // Batch load
        if (generatedJsonDocuments.size() >= bucketUpsertConfiguration.getBatchLimit()) {
            List<JsonDocument> copy = new ArrayList<>(generatedJsonDocuments);
            Observable.from(copy).flatMap(
                    (final JsonDocument docToInsert) -> bucketUpsertConfiguration.getBucket().async()
                            .upsert(docToInsert).retryWhen(RetryBuilder.anyOf(Exception.class).delay(Delay
                                    .fixed(bucketUpsertConfiguration.getFailureRetryDelay(), TimeUnit.MILLISECONDS))
                                    .max(bucketUpsertConfiguration.getFailureMaximumRetries()).build()))
                    .onErrorReturn(throwable -> {
                        System.out.println(throwable.getMessage());
                        return null;
                    }).toBlocking().last();
            generatedJsonDocuments = new ArrayList<>();
        }
    }
}