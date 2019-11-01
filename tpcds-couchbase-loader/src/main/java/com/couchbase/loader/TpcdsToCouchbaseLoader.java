/*
 * Copyright 2019 Couchbase, Inc.
 */
package com.couchbase.loader;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.couchbase.client.core.env.KeyValueServiceConfig;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.teradata.tpcds.Session;

/**
 * This class will generate TPC-DS data based on the specified partitions and scale factor, and then load it into the
 * Couchbase buckets.
 * If the partition number is passed as an argument, then only that partition is generated and loaded to the bucket. If
 * the partition number is not passed, or a value of -1 is passed, then all the partitions will be generated and loaded
 * to the bucket.
 */
public class TpcdsToCouchbaseLoader {

    private static final Logger LOGGER = LogManager.getRootLogger();

    // Configuration file name
    private static final String PROPERTIES_FILE_NAME = "tpcds.properties";

    // Couchbase cluster and bucket configs default values
    private static final String HOST_NAME_DEFAULT = "localhost";
    private static final int PORT_DEFAULT = -9999; // Let the SDK use its own default if not provided
    private static final String USER_NAME_DEFAULT = "Administrator";
    private static final String PASSWORD_DEFAULT = "couchbase";
    private static final String BUCKET_NAME_DEFAULT = "tpcds";
    private static final boolean IS_DELETE_BUCKET_IF_EXISTS_DEFAULT = true;
    private static final int MEMORY_QUOTA_DEFAULT = 4096; // In megabytes

    // Configurable members default values
    private static final int BATCH_LIMIT_DEFAULT = 10000; // Threshold to reach before batch upserting
    private static final double SCALE_FACTOR_DEFAULT = 1;
    private static final int PARTITIONS_DEFAULT = 2;
    private static final int PARTITION_DEFAULT = -1;
    private static final int KV_ENDPOINTS_DEFAULT = 5; // improves the pipelining for better performance
    private static final int KV_TIMEOUT_DEFAULT = 30000;
    private static final int FAILURE_RETRY_DELAY_DEFAULT = 5000;
    private static final int FAILURE_MAXIMUM_RETRIES_DEFAULT = 10;
    private static final boolean ENABLE_PADDING_DEFAULT = false;

    // Properties field names
    private static final String PROPERTIES_FILE_PATH_FIELD_NAME = "propertiesfilepath";
    private static final String HOST_NAME_FIELD_NAME = "hostname";
    private static final String PORT_FIELD_NAME = "port";
    private static final String USER_NAME_FIELD_NAME = "username";
    private static final String PASSWORD_FIELD_NAME = "password";
    private static final String BUCKET_NAME_FIELD_NAME = "bucketname";
    private static final String IS_DELETE_IF_BUCKET_EXISTS_FIELD_NAME = "isdeleteifbucketexists";
    private static final String MEMORY_QUOTA_FIELD_NAME = "memoryquota";
    private static final String BATCH_LIMIT_FIELD_NAME = "batchlimit";
    private static final String SCALE_FACTOR_FIELD_NAME = "scalefactor";
    private static final String PARTITIONS_FIELD_NAME = "partitions";
    private static final String PARTITION_FIELD_NAME = "partition";
    private static final String KV_ENDPOINTS_FIELD_NAME = "kvendpoints";
    private static final String KV_TIMEOUT_FIELD_NAME = "kvtimeout";
    private static final String FAILURE_RETRY_DELAY_FIELD_NAME = "failureretrydelay";
    private static final String FAILURE_MAXIMUM_RETRIES_FIELD_NAME = "failuremaximumretries";
    private static final String ENABLE_PADDING_FIELD_NAME = "enablepadding";

    private static final Map<String, String> configuration = new HashMap<>();

    static {
        configuration.put(HOST_NAME_FIELD_NAME, HOST_NAME_DEFAULT);
        configuration.put(PORT_FIELD_NAME, String.valueOf(PORT_DEFAULT));
        configuration.put(USER_NAME_FIELD_NAME, USER_NAME_DEFAULT);
        configuration.put(PASSWORD_FIELD_NAME, PASSWORD_DEFAULT);
        configuration.put(BUCKET_NAME_FIELD_NAME, BUCKET_NAME_DEFAULT);
        configuration.put(IS_DELETE_IF_BUCKET_EXISTS_FIELD_NAME, String.valueOf(IS_DELETE_BUCKET_IF_EXISTS_DEFAULT));
        configuration.put(MEMORY_QUOTA_FIELD_NAME, String.valueOf(MEMORY_QUOTA_DEFAULT));
        configuration.put(BATCH_LIMIT_FIELD_NAME, String.valueOf(BATCH_LIMIT_DEFAULT));
        configuration.put(SCALE_FACTOR_FIELD_NAME, String.valueOf(SCALE_FACTOR_DEFAULT));
        configuration.put(PARTITIONS_FIELD_NAME, String.valueOf(PARTITIONS_DEFAULT));
        configuration.put(PARTITION_FIELD_NAME, String.valueOf(PARTITION_DEFAULT));
        configuration.put(KV_ENDPOINTS_FIELD_NAME, String.valueOf(KV_ENDPOINTS_DEFAULT));
        configuration.put(KV_TIMEOUT_FIELD_NAME, String.valueOf(KV_TIMEOUT_DEFAULT));
        configuration.put(FAILURE_RETRY_DELAY_FIELD_NAME, String.valueOf(FAILURE_RETRY_DELAY_DEFAULT));
        configuration.put(FAILURE_MAXIMUM_RETRIES_FIELD_NAME, String.valueOf(FAILURE_MAXIMUM_RETRIES_DEFAULT));
        configuration.put(ENABLE_PADDING_FIELD_NAME, String.valueOf(ENABLE_PADDING_DEFAULT));
    }

    // Table to generate, null value will generate all tables
    private static String TABLE_TO_GENERATE = null;

    public static void main(String[] args) {
        // Read the configuration from the command line and properties file
        readConfiguration(args);

        // Threads count is based on the partitions level
        // If partition number is not -1, then this is meant to generate a single partition, so we have a single thread
        int partitions = Integer.valueOf(configuration.get(PARTITIONS_FIELD_NAME));
        int partition = Integer.valueOf(configuration.get(PARTITION_FIELD_NAME));

        ExecutorService executorService = Executors.newFixedThreadPool(partition == -1 ? partitions : 1);
        LOGGER.info("Number of partitions is " + partitions);

        // Create and authenticate the cluster
        Cluster cluster = createAndAuthenticateCluster();

        // Open the bucket
        Bucket bucket = openBucket(cluster);

        // Start time (for statistics)
        long startTime = System.currentTimeMillis();

        // partition -1 will result in generating all partitions on a single instance
        if (partition == -1) {
            // We start from 1 since partition numbers start from 1
            generateAllPartitions(executorService, bucket);
        } else {
            generateSinglePartition(executorService, bucket);
        }

        // Wait for all partitions to finish their work
        executorService.shutdown();
        try {
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
            executorService.shutdownNow();
        } catch (Exception ex) {
            executorService.shutdownNow();
            LOGGER.error(ex.getMessage());
        }

        // End time (for statistics)
        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime) / 1000.00;
        LOGGER.info("Total time: " + duration + " seconds");
        LOGGER.info("Data generation completed");

        // Release all resources
        cluster.disconnect();
    }

    /**
     * Reads the configuration for the data generation.
     *
     * @param arguments Passed command-line arguments
     */
    private static void readConfiguration(String[] arguments) {
        // Command line configurations
        Map<String, String> cmdlineConfig = readCommandLineConfiguration(arguments);

        String propertiesFilePath = null;

        // Get the properties file if it was provided in command line arguments
        for (Map.Entry<String, String> entry : cmdlineConfig.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(PROPERTIES_FILE_PATH_FIELD_NAME)) {
                propertiesFilePath = entry.getValue();
                break;
            }
        }

        // Properties file configurations
        Map<String, String> propertiesFileConfig = readPropertiesFileConfiguration(propertiesFilePath);

        // Make sure the command line configs override the properties file configs
        configuration.putAll(propertiesFileConfig);
        configuration.putAll(cmdlineConfig);
    }

    /**
     * Reads the configuration passed in the command line
     *
     * @param arguments command line arguments
     * @return a map containing the read configuration
     */
    private static Map<String, String> readCommandLineConfiguration(String[] arguments) {
        Map<String, String> configs = new HashMap<>();

        // Arguments from command line
        if (arguments != null && arguments.length > 0) {
            for (String arg : arguments) {
                if (arg.contains("=")) {
                    configs.put(arg.substring(0, arg.indexOf('=')).toLowerCase(), arg.substring(arg.indexOf('=') + 1));
                }
            }
        }

        return configs;
    }

    /**
     * Reads the configuration from the properties file. Uses the default properties file if path for properties file
     * is not provided.
     *
     * @param propertiesFilePath properties file path that is provided by the user
     * @return a map containing the read configuration
     */
    private static Map<String, String> readPropertiesFileConfiguration(String propertiesFilePath) {
        Map<String, String> configs = new HashMap<>();
        boolean isPropertiesFilePathProvided = false;

        // User provided file path
        if (propertiesFilePath != null) {
            isPropertiesFilePathProvided = true;
            LOGGER.info("Loaded properties file: " + propertiesFilePath);
        } else {
            // No file provided, use default
            LOGGER.info("No properties file provided, using default properties file");
        }

        try (InputStream inputStream = isPropertiesFilePathProvided ?
                new FileInputStream(Paths.get(propertiesFilePath).toFile()) :
                TpcdsToCouchbaseLoader.class.getClassLoader().getResourceAsStream(PROPERTIES_FILE_NAME)) {

            Properties properties = new Properties();

            // Load the properties from the configuration file
            if (inputStream != null) {
                properties.load(inputStream);
            } else {
                // This should never happen, default configuration file exists in resources
                LOGGER.error("Properties configuration file not found");
            }

            for (Map.Entry<Object, Object> entry : properties.entrySet()) {
                configs.put(entry.getKey().toString().toLowerCase(), entry.getValue().toString());
            }
        } catch (FileNotFoundException ex) {
            LOGGER.error("Configuration file not found. " + ex.getMessage());
        } catch (Exception ex) {
            LOGGER.error("Failed to read configuration file. " + ex.getMessage());
        }

        return configs;
    }

    /**
     * Create and authenticate the cluster.
     * Note: Actual connection to the cluster only happens when operations are attempted, e.g, opening a bucket.
     *
     * @return Returns the Couchbase cluster.
     */
    private static Cluster createAndAuthenticateCluster() {
        String hostname = configuration.get(HOST_NAME_FIELD_NAME);
        String username = configuration.get(USER_NAME_FIELD_NAME);
        String password = configuration.get(PASSWORD_FIELD_NAME);
        int port = Integer.valueOf(configuration.get(PORT_FIELD_NAME));
        int kvEndpoints = Integer.valueOf(configuration.get(KV_ENDPOINTS_FIELD_NAME));
        int kvTimeout = Integer.valueOf(configuration.get(KV_TIMEOUT_FIELD_NAME));

        DefaultCouchbaseEnvironment.Builder builder = DefaultCouchbaseEnvironment.builder().kvTimeout(kvTimeout)
                .keyValueServiceConfig(KeyValueServiceConfig.create(kvEndpoints)).continuousKeepAliveEnabled(false);

        // Use the provided port if supplied
        if (port != PORT_DEFAULT) {
            builder.bootstrapHttpDirectPort(port);
        }

        // Build the environment
        LOGGER.info("Provided properties: " + configuration.toString());
        CouchbaseEnvironment environment = builder.build();

        String address = "hostname: " + hostname + " port: " + environment.bootstrapHttpDirectPort();
        LOGGER.info("Creating and authenticating cluster on: " + address);
        Cluster cluster = CouchbaseCluster.create(environment, hostname);
        cluster.authenticate(username, password);
        LOGGER.info("Cluster created and authenticated successfully on: " + address);

        return cluster;
    }

    /**
     * Opens the bucket.
     *
     * @param cluster Cluster which the bucket is created on.
     * @return Returns the bucket to upsert the data to.
     */
    private static Bucket openBucket(Cluster cluster) {
        String bucketName = configuration.get(BUCKET_NAME_FIELD_NAME);

        LOGGER.info("opening bucket " + bucketName);
        Bucket bucket = cluster.openBucket(bucketName);
        LOGGER.info(bucketName + " bucket opened");

        return bucket;
    }

    /**
     * When the requested partition is -1, it indicates that this instance needs to generate all partitions on its own
     * (depending on the parallelism level provided)
     *
     * @param executorService ExecutorService running the Runnables
     * @param bucket          The bucket the upsert operation is applied to
     */
    private static void generateAllPartitions(ExecutorService executorService, Bucket bucket) {
        int partitions = Integer.valueOf(configuration.get(PARTITIONS_FIELD_NAME));
        double scaleFactor = Double.valueOf(configuration.get(SCALE_FACTOR_FIELD_NAME));
        int batchLimit = Integer.valueOf(configuration.get(BATCH_LIMIT_FIELD_NAME));
        int failureRetryDelay = Integer.valueOf(configuration.get(FAILURE_RETRY_DELAY_FIELD_NAME));
        int failureMaximumRetries = Integer.valueOf(configuration.get(FAILURE_MAXIMUM_RETRIES_FIELD_NAME));
        boolean enablePadding = Boolean.valueOf(configuration.get(ENABLE_PADDING_FIELD_NAME));

        for (int i = 1; i <= partitions; i++) {
            Session session =
                    Session.getDefaultSession().withScale(scaleFactor).withParallelism(partitions).withChunkNumber(i);

            TpcdsConfiguration tpcdsConfiguration =
                    new TpcdsConfiguration(session, i, TABLE_TO_GENERATE, enablePadding);
            BucketConfiguration bucketConfiguration =
                    new BucketConfiguration(bucket, batchLimit, failureRetryDelay, failureMaximumRetries);
            TpcdsGeneratorAndLoaderRunnable runnable =
                    new TpcdsGeneratorAndLoaderRunnable(tpcdsConfiguration, bucketConfiguration);
            executorService.submit(runnable);
        }
    }

    /**
     * When the requested partition is not -1, it indicates that the provided partition only needs to be generated.
     *
     * @param executorService ExecutorService running the Runnables
     * @param bucket          The bucket the upsert operation is applied to
     */
    private static void generateSinglePartition(ExecutorService executorService, Bucket bucket) {
        int partitions = Integer.valueOf(configuration.get(PARTITIONS_FIELD_NAME));
        int partition = Integer.valueOf(configuration.get(PARTITION_FIELD_NAME));
        double scaleFactor = Double.valueOf(configuration.get(SCALE_FACTOR_FIELD_NAME));
        int batchLimit = Integer.valueOf(configuration.get(BATCH_LIMIT_FIELD_NAME));
        int failureRetryDelay = Integer.valueOf(configuration.get(FAILURE_RETRY_DELAY_FIELD_NAME));
        int failureMaximumRetries = Integer.valueOf(configuration.get(FAILURE_MAXIMUM_RETRIES_FIELD_NAME));
        boolean enablePadding = Boolean.valueOf(configuration.get(ENABLE_PADDING_FIELD_NAME));

        // Generating a single partition
        Session session = Session.getDefaultSession().withScale(scaleFactor).withParallelism(partitions)
                .withChunkNumber(partition);

        TpcdsConfiguration tpcdsConfiguration =
                new TpcdsConfiguration(session, partition, TABLE_TO_GENERATE, enablePadding);
        BucketConfiguration bucketConfiguration =
                new BucketConfiguration(bucket, batchLimit, failureRetryDelay, failureMaximumRetries);
        TpcdsGeneratorAndLoaderRunnable runnable =
                new TpcdsGeneratorAndLoaderRunnable(tpcdsConfiguration, bucketConfiguration);
        executorService.submit(runnable);
    }
}
