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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import rx.Observable;
import com.couchbase.utils.Utils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * The Runnable used to generate the data. This will generate the data and load it into KV based on the provided
 * configuration.
 */
public class TpcdsGeneratorAndLoaderRunnable implements Runnable {

    private static final Logger LOGGER = LogManager.getRootLogger();

    // Table name will be added to each generated record
    private final static String TABLE_NAME_FIELD_NAME = "table_name";

    // When generating the values, a list is created, at index 0, all the values for the parent record exist, if a
    // child record is created, it is at index 1 in the list
    private static final int PARENT_VALUES_INDEX = 0;
    private static final int CHILD_VALUES_INDEX = 1;

    // Statistics members
    private int recordCount;
    private long bytesBeforeJsonWithoutTableNameField;
    private long bytesBeforeJsonWithTableNameField;
    private long bytesAfterJson;

    // Table members
    private final List<Table> selectedTables;
    private final List<Iterator<List<List<String>>>> tableIterators = new ArrayList<>();
    private Table currentTable;
    private final int tableCount;
    private int currentTableIndex;
    private final boolean generateAllTables;

    // Configuration
    private TpcdsConfiguration tpcdsConfiguration;
    private BucketConfiguration bucketConfiguration;

    // List to batch the insertion
    private ArrayList<JsonDocument> generatedJsonDocuments = new ArrayList<>();

    TpcdsGeneratorAndLoaderRunnable(TpcdsConfiguration tpcdsConfiguration, BucketConfiguration bucketConfiguration) {
        this.tpcdsConfiguration = tpcdsConfiguration;
        this.bucketConfiguration = bucketConfiguration;

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

    @Override
    public void run() {
        // Set the thread name to be partition being generated
        String originalThreadName = Thread.currentThread().getName();
        Thread.currentThread().setName("Partition " + tpcdsConfiguration.getPartition());

        boolean continueGeneration;

        // Loop until the conditions stop the loop, we stop once all the tables are generated for this partition
        while (true) {
            continueGeneration = false;

            // Current table still has more
            if (tableIterators.get(currentTableIndex).hasNext()) {
                continueGeneration = true;
            }
            // We went over all the tables, stop the generation
            else if (currentTableIndex == tableCount - 1) {
                break;
            }

            // Flag is true, generate the next one
            if (continueGeneration) {
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
                    (final JsonDocument docToInsert) -> bucketConfiguration.getBucket().async().upsert(docToInsert)
                            .retryWhen(RetryBuilder.anyOf(Exception.class).delay(Delay
                                    .fixed(bucketConfiguration.getFailureRetryDelay(), TimeUnit.MILLISECONDS))
                                    .max(bucketConfiguration.getFailureMaximumRetries()).build()))
                    .onErrorReturn(throwable -> {
                        LOGGER.error(throwable.getMessage());
                        return null;
                    }).toBlocking().last();
        }

        LOGGER.info("Partition " + tpcdsConfiguration.getPartition() + " generated " + recordCount + " records");
        LOGGER.info("Partition " + tpcdsConfiguration.getPartition()
                + " Size Before Json String (Values only, no table name): " + bytesBeforeJsonWithoutTableNameField
                + " bytes");
        LOGGER.info("Partition " + tpcdsConfiguration.getPartition()
                + " Size Before Json String (Values only, with table name): " + bytesBeforeJsonWithTableNameField
                + " bytes");
        LOGGER.info(
                "Partition " + tpcdsConfiguration.getPartition() + " Size After Json String (Field names + Values): "
                        + bytesAfterJson + " bytes");

        // Set the thread back to its original name
        Thread.currentThread().setName(originalThreadName);
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
        recordCount++;

        // Construct the record in JSON
        JsonObject parentRecord = constructRecord(values.get(PARENT_VALUES_INDEX), currentTable);

        // Counting total size in bytes for generated values (after Json conversion)
        bytesAfterJson += parentRecord.toString().length();

        // JsonDocument with key and created record
        JsonDocument parentJsonDocument = JsonDocument
                .create(currentTable.getName() + "-" + ((tpcdsConfiguration.getPartition() - 1) + (recordCount
                        * tpcdsConfiguration.getPartitions())), parentRecord);

        // Collecting records for batch upsert
        generatedJsonDocuments.add(parentJsonDocument);

        // In some cases, the generator generates 2 records, one for current table, and one for child table.
        if (generateAllTables && values.size() > 1) {

            // increment the counter for child record
            recordCount++;

            // Construct the record in JSON
            JsonObject childRecord = constructRecord(values.get(CHILD_VALUES_INDEX), currentTable.getChild());

            // Counting total size in bytes for generated values (after Json conversion)
            bytesAfterJson += childRecord.toString().length();

            // JsonDocument with key and created record
            JsonDocument childJsonDocument = JsonDocument
                    .create(currentTable.getChild().getName() + "-" + ((tpcdsConfiguration.getPartition() - 1) + (
                            recordCount * tpcdsConfiguration.getPartitions())), childRecord);

            // Collecting records for batch upsert
            generatedJsonDocuments.add(childJsonDocument);
        }

        // Batch load
        if (generatedJsonDocuments.size() >= bucketConfiguration.getBatchLimit()) {
            List<JsonDocument> copy = new ArrayList<>(generatedJsonDocuments);
            Observable.from(copy).flatMap(
                    (final JsonDocument docToInsert) -> bucketConfiguration.getBucket().async().upsert(docToInsert)
                            .retryWhen(RetryBuilder.anyOf(Exception.class).delay(Delay
                                    .fixed(bucketConfiguration.getFailureRetryDelay(), TimeUnit.MILLISECONDS))
                                    .max(bucketConfiguration.getFailureMaximumRetries()).build()))
                    .onErrorReturn(throwable -> {
                        LOGGER.error(throwable.getMessage());
                        return null;
                    }).toBlocking().last();
            generatedJsonDocuments = new ArrayList<>();
        }
    }

    /**
     * Constructs the record with the appropriate data types.
     *
     * @param values list containing all the generated values for all columns in a string format.
     * @param table  Table the record is being constructed for
     *
     * @return Returns a JsonObject containing the values converted to their appropriate data types.
     */
    private JsonObject constructRecord(List<String> values, Table table) {
        JsonObject record = JsonObject.empty();

        // Add the table name to the record
        record.put(TABLE_NAME_FIELD_NAME, table.getName());

        // Including table name length in the total size
        bytesBeforeJsonWithTableNameField += table.getName().length();

        // Build the record data
        for (int counter = 0; counter < values.size(); counter++) {

            // If the value is null, no need to check for the column type
            if (values.get(counter) == null) {
                record.put(table.getColumns()[counter].getName(), values.get(counter));
                continue;
            }

            String fieldName = table.getColumns()[counter].getName();
            String stringValue = values.get(counter);

            // Convert the value to the appropriate type based on the column type
            switch (table.getColumns()[counter].getType().getBase()) {
                // Identifier could be any value, so we're taking it as a string
                case IDENTIFIER:
                    record.put(fieldName, stringValue);
                    break;
                // Date and Time are not supported, they are stored as strings and can be modified with date functions
                case CHAR:
                case VARCHAR:
                case DATE:
                case TIME:
                    // Precision indicating the length that should be applied to the String value
                    Optional<Integer> varcharPrecision = table.getColumns()[counter].getType().getPrecision();

                    // Padding is enabled, add white space trailing if needed
                    if (varcharPrecision.isPresent() && tpcdsConfiguration.isEnablePadding()) {
                        record.put(fieldName, Utils.createStringWithPadding(stringValue, varcharPrecision.get()));
                    } else {
                        // Precision not provided or padding is disabled from configuration
                        record.put(fieldName, stringValue);
                    }
                    break;
                case INTEGER:
                    record.put(fieldName, Integer.valueOf(stringValue));
                    break;
                case DECIMAL:
                    record.put(fieldName, Double.valueOf(stringValue));
                    break;
                default:
                    record.put(fieldName, stringValue);
                    break;

            }

            // Counting total size in bytes for generated values (before Json conversion)
            if (values.get(counter) != null) {
                bytesBeforeJsonWithTableNameField += values.get(counter).length();
                bytesBeforeJsonWithoutTableNameField += values.get(counter).length();
            }
        }

        return record;
    }
}