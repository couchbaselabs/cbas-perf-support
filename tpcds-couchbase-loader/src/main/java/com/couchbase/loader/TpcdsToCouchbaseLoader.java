/*
 * Copyright 2019 Couchbase, Inc.
 */
package com.couchbase.loader;

import java.io.FileNotFoundException;
import java.io.InputStream;
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
 * This class will generate TPC-DS data based on the specified partitions and scaling factor, and then load it into the
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
    private static final boolean DELETE_BUCKET_IF_EXISTS_DEFAULT = true;
    private static final int BUCKET_MEMORY_QUOTA_DEFAULT = 4096; // In megabytes

    // Configurable members default values
    private static final int BATCH_LIMIT_DEFAULT = 10000; // Threshold to reach before batch upserting
    private static final double SCALING_FACTOR_DEFAULT = 1;
    private static final int PARTITIONS_DEFAULT = 2;
    private static final int PARTITION_DEFAULT = -1;
    private static final int KV_ENDPOINTS_DEFAULT = 5; // improves the pipelining for better performance
    private static final int KV_TIMEOUT_DEFAULT = 10000;
    private static final int FAILURE_RETRY_DELAY_DEFAULT = 5000;
    private static final int FAILURE_MAXIMUM_RETRIES_DEFAULT = 10;
    private static final boolean ENABLE_PADDING_DEFAULT = false;

    // Properties field names
    private static final String HOST_NAME_FIELD_NAME = "hostname";
    private static final String PORT_FIELD_NAME = "port";
    private static final String USER_NAME_FIELD_NAME = "username";
    private static final String PASSWORD_FIELD_NAME = "password";
    private static final String BUCKET_NAME_FIELD_NAME = "bucketname";
    private static final String IS_DELETE_IF_BUCKET_EXISTS_FIELD_NAME = "isdeleteifbucketexists";
    private static final String BUCKET_SIZE_FIELD_NAME = "memoryquota";
    private static final String BATCH_LIMIT_FIELD_NAME = "batchlimit";
    private static final String SCALING_FACTOR_FIELD_NAME = "scalingfactor";
    private static final String PARTITIONS_FIELD_NAME = "partitions";
    private static final String PARTITION_FIELD_NAME = "partition";
    private static final String KV_ENDPOINTS_FIELD_NAME = "kvendpoints";
    private static final String KV_TIMEOUT_FIELD_NAME = "kvtimeout";
    private static final String FAILURE_RETRY_DELAY_FIELD_NAME = "failureretrydelay";
    private static final String FAILURE_MAXIMUM_RETRIES_FIELD_NAME = "failuremaximumretries";
    private static final String ENABLE_PADDING_FIELD_NAME = "enablepadding";

    // Any configuration values that are not passed will use their default respective value
    private static String hostname = HOST_NAME_DEFAULT;
    private static int port = PORT_DEFAULT;
    private static String username = USER_NAME_DEFAULT;
    private static String password = PASSWORD_DEFAULT;
    private static String bucketName = BUCKET_NAME_DEFAULT;
    private static boolean isDeleteBucketIfExists = DELETE_BUCKET_IF_EXISTS_DEFAULT;
    private static int memoryQuota = BUCKET_MEMORY_QUOTA_DEFAULT;
    private static int batchLimit = BATCH_LIMIT_DEFAULT;
    private static double scalingFactor = SCALING_FACTOR_DEFAULT;
    private static int partitions = PARTITIONS_DEFAULT;
    private static int partition = PARTITION_DEFAULT;
    private static int kvEndpoints = KV_ENDPOINTS_DEFAULT;
    private static int kvTimeout = KV_TIMEOUT_DEFAULT;
    private static int failureRetryDelay = FAILURE_RETRY_DELAY_DEFAULT;
    private static int failureMaximumRetries = FAILURE_MAXIMUM_RETRIES_DEFAULT;
    private static boolean enablePadding = ENABLE_PADDING_DEFAULT;

    // Table to generate, null value will generate all tables
    private static String TABLE_TO_GENERATE = null;

    public static void main(String[] args) {
        Map<String, String> configuration = new HashMap<>();

        // Load the configurations from properties file and command line arguments
        loadConfiguration(args, configuration);

        // Read the provided arguments and overwrite the default values if necessary
        readConfiguration(configuration);

        // Threads count is based on the partitions level
        // If partition number is not -1, then this is meant to generate a single partition, so we have a single thread
        ExecutorService executorService = Executors.newFixedThreadPool(partition == -1 ? partitions : 1);
        LOGGER.info("partitions level is " + partitions);

        // Connect to the server and authenticate
        Cluster cluster = connectAndAuthenticate();

        // Get the created bucket
        Bucket bucket = openBucket(cluster);

        // Start time (for statistics)
        long startTime = System.currentTimeMillis();

        // partition -1 will result in generating all partitions on a single partition
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
     * Reads the configuration from a properties file, then override it with command line arguments. Any parameters
     * that are not provided will use their default values.
     */
    private static void loadConfiguration(String[] arguments, Map<String, String> configuration) {

        // First, read the arguments from the properties file
        try (InputStream inputStream = TpcdsToCouchbaseLoader.class.getClassLoader()
                .getResourceAsStream(PROPERTIES_FILE_NAME)) {
            Properties properties = new Properties();

            if (inputStream != null) {
                properties.load(inputStream);
                for (Map.Entry<Object, Object> entry : properties.entrySet()) {
                    configuration.put(entry.getKey().toString().toLowerCase(), entry.getValue().toString());
                }
            }
        } catch (FileNotFoundException ex) {
            LOGGER.error("Configuration file not found");
        } catch (Exception ex) {
            LOGGER.error("Failed to read configuration file");
        }

        // Second, override any properties with anything that is provided in command line arguments
        if (arguments != null && arguments.length > 0) {
            for (String arg : arguments) {
                if (arg.contains("=")) {
                    configuration
                            .put(arg.substring(0, arg.indexOf('=')).toLowerCase(), arg.substring(arg.indexOf('=') + 1));
                }
            }
        }
    }

    /**
     * Configuration values are set to their default value, if no new configuration is passed, the default value is
     * used.
     */
    private static void readConfiguration(Map<String, String> config) {
        hostname = config.get(HOST_NAME_FIELD_NAME) != null ? config.get(HOST_NAME_FIELD_NAME) : hostname;
        port = config.get(PORT_FIELD_NAME) != null ? Integer.valueOf(config.get(PORT_FIELD_NAME)) : port;
        username = config.get(USER_NAME_FIELD_NAME) != null ? config.get(USER_NAME_FIELD_NAME) : username;
        password = config.get(PASSWORD_FIELD_NAME) != null ? config.get(PASSWORD_FIELD_NAME) : password;
        bucketName = config.get(BUCKET_NAME_FIELD_NAME) != null ? config.get(BUCKET_NAME_FIELD_NAME) : bucketName;
        isDeleteBucketIfExists = config.get(IS_DELETE_IF_BUCKET_EXISTS_FIELD_NAME) != null ?
                Boolean.valueOf(config.get(IS_DELETE_IF_BUCKET_EXISTS_FIELD_NAME)) :
                isDeleteBucketIfExists;
        memoryQuota = config.get(BUCKET_SIZE_FIELD_NAME) != null ?
                Integer.valueOf(config.get(BUCKET_SIZE_FIELD_NAME)) :
                memoryQuota;
        batchLimit = config.get(BATCH_LIMIT_FIELD_NAME) != null ?
                Integer.valueOf(config.get(BATCH_LIMIT_FIELD_NAME)) :
                batchLimit;
        scalingFactor = config.get(SCALING_FACTOR_FIELD_NAME) != null ?
                Double.valueOf(config.get(SCALING_FACTOR_FIELD_NAME)) :
                scalingFactor;
        partitions = config.get(PARTITIONS_FIELD_NAME) != null ?
                Integer.valueOf(config.get(PARTITIONS_FIELD_NAME)) :
                partitions;
        partition = config.get(PARTITION_FIELD_NAME) != null ?
                Integer.valueOf(config.get(PARTITION_FIELD_NAME)) :
                partition;
        kvEndpoints = config.get(KV_ENDPOINTS_FIELD_NAME) != null ?
                Integer.valueOf(config.get(KV_ENDPOINTS_FIELD_NAME)) :
                kvEndpoints;
        kvTimeout = config.get(KV_TIMEOUT_FIELD_NAME) != null ?
                Integer.valueOf(config.get(KV_TIMEOUT_FIELD_NAME)) :
                kvTimeout;
        failureRetryDelay = config.get(FAILURE_RETRY_DELAY_FIELD_NAME) != null ?
                Integer.valueOf(config.get(FAILURE_RETRY_DELAY_FIELD_NAME)) :
                failureRetryDelay;
        failureMaximumRetries = config.get(FAILURE_MAXIMUM_RETRIES_FIELD_NAME) != null ?
                Integer.valueOf(config.get(FAILURE_MAXIMUM_RETRIES_FIELD_NAME)) :
                failureMaximumRetries;
        enablePadding = config.get(ENABLE_PADDING_FIELD_NAME) != null ?
                Boolean.valueOf(config.get(ENABLE_PADDING_FIELD_NAME)) :
                enablePadding;
    }

    /**
     * Connects to the server and authenticates.
     *
     * @return Returns the Couchbase cluster.
     */
    private static Cluster connectAndAuthenticate() {
        LOGGER.info("Connecting to Couchbase server");
        DefaultCouchbaseEnvironment.Builder builder = DefaultCouchbaseEnvironment.builder().kvTimeout(kvTimeout)
                .keyValueServiceConfig(KeyValueServiceConfig.create(kvEndpoints)).continuousKeepAliveEnabled(false);

        // Use the provided port if supplied
        if (port != PORT_DEFAULT) {
            builder.bootstrapHttpDirectPort(port);
        }

        CouchbaseEnvironment environment = builder.build();
        Cluster cluster = CouchbaseCluster.create(environment, hostname);
        cluster.authenticate(username, password);
        LOGGER.info("Connection to Couchbase server successful");

        return cluster;
    }

    /**
     * Opens the bucket to upsert the data to.
     *
     * @param cluster Cluster which the bucket is created on.
     * @return Returns the bucket to upsert the data to.
     */
    private static Bucket openBucket(Cluster cluster) {
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
        for (int i = 1; i <= partitions; i++) {
            Session session =
                    Session.getDefaultSession().withScale(scalingFactor).withParallelism(partitions).withChunkNumber(i);

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
        // Generating a single partition
        Session session = Session.getDefaultSession().withScale(scalingFactor).withParallelism(partitions)
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
