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

import org.apache.logging.log4j.Level;
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
 * This class will generate TPC-DS data based on the specified parallelism and scaling factor, and then feed it to the
 * KV buckets.
 *
 * If the chunk number is passed as an argument, then only that chunk is generated and loaded to the KV bucket. If the
 * chunk number is not passed, or a value of -1 is passed, then all the chunks will be generated and loaded to the KV
 * bucket.
 *
 * Note:
 * For convenience, chunk number is equal to partition/thread number and they start at 1, and hence, partition 1
 * generates chunk 1, partition 2 generates chunk 2, ... and so on.
 */
public class TpcdsToKVFeeder {

    private static final Logger LOGGER = LogManager.getRootLogger();

    // Configuration file name
    private static final String PROPERTIES_FILE_NAME = "tpcds.properties";

    // Couchbase cluster and bucket configs default values
    private static final String HOST_NAME_DEFAULT = "localhost";
    private static final String USER_NAME_DEFAULT = "Administrator";
    private static final String PASSWORD_DEFAULT = "couchbase";
    private static final String BUCKET_NAME_DEFAULT = "tpcds";
    private static final boolean DELETE_BUCKET_IF_EXISTS_DEFAULT = true;
    private static final int BUCKET_SIZE_DEFAULT = 4096; // In megabytes

    // Configurable members default values
    private static final int BATCH_LIMIT_DEFAULT = 10000; // Threshold to reach before batch upserting
    private static final double SCALING_FACTOR_DEFAULT = 1;
    private static final int PARALLELISM_DEFAULT = 2;
    private static final int CHUNK_NUMBER_DEFAULT = -1;
    private static final int KV_ENDPOINTS_DEFAULT = 5; // improves the pipelining for better performance
    private static final int KV_TIMEOUT_DEFAULT = 10000;
    private static final int FAILURE_RETRY_DELAY_DEFAULT = 5000;

    // Properties field names
    private static final String HOST_NAME_FIELD_NAME = "hostname";
    private static final String USER_NAME_FIELD_NAME = "username";
    private static final String PASSWORD_FIELD_NAME = "password";
    private static final String BUCKET_NAME_FIELD_NAME = "bucketname";
    private static final String IS_DELETE_IF_BUCKET_EXISTS_FIELD_NAME = "isdeleteifbucketexists";
    private static final String BUCKET_SIZE_FIELD_NAME = "bucketsize";
    private static final String BATCH_LIMIT_FIELD_NAME = "batchlimit";
    private static final String SCALING_FACTOR_FIELD_NAME = "scalingfactor";
    private static final String PARALLELISM_FIELD_NAME = "parallelism";
    private static final String CHUNK_NUMBER_FIELD_NAME = "chunknumber";
    private static final String KV_ENDPOINTS_FIELD_NAME = "kvendpoints";
    private static final String KV_TIMEOUT_FIELD_NAME = "kvtimeout";
    private static final String FAILURE_RETRY_DELAY_FIELD_NAME = "failureRetryDelay";

    // Any configuration values that are not passed will use their default respective value
    private static String hostname = HOST_NAME_DEFAULT;
    private static String username = USER_NAME_DEFAULT;
    private static String password = PASSWORD_DEFAULT;
    private static String bucketName = BUCKET_NAME_DEFAULT;
    private static boolean isDeleteBucketIfExists = DELETE_BUCKET_IF_EXISTS_DEFAULT;
    private static int bucketSize = BUCKET_SIZE_DEFAULT;
    private static int batchLimit = BATCH_LIMIT_DEFAULT;
    private static double scalingFactor = SCALING_FACTOR_DEFAULT;
    private static int parallelism = PARALLELISM_DEFAULT;
    private static int kvEndpoints = KV_ENDPOINTS_DEFAULT;
    private static int kvTimeout = KV_TIMEOUT_DEFAULT;
    private static int failureRetryDelay = FAILURE_RETRY_DELAY_DEFAULT;
    private static int chunkNumber = CHUNK_NUMBER_DEFAULT;

    // Table to generate, null value will generate all tables
    private static String TABLE_TO_GENERATE = null;

    public static void main(String[] args) {
        Map<String, String> configuration = new HashMap<>();

        // Load the configuration values and read them
        loadConfiguration(args, configuration);
        readConfiguration(configuration);

        // Threads count is based on the parallelism level
        // If chunk number is not -1, then this is meant to generate a single chunk, so we have a single thread
        ExecutorService executorService = Executors.newFixedThreadPool(chunkNumber == -1 ? parallelism : 1);
        LOGGER.log(Level.INFO, "Parallelism level is " + parallelism);

        // Connect to the server and authenticate
        LOGGER.log(Level.INFO, "Connecting to Couchbase server");
        CouchbaseEnvironment environment = DefaultCouchbaseEnvironment.builder().kvTimeout(kvTimeout)
                .keyValueServiceConfig(KeyValueServiceConfig.create(kvEndpoints)).continuousKeepAliveEnabled(false)
                .build();
        Cluster cluster = CouchbaseCluster.create(environment, hostname);
        cluster.authenticate(username, password);
//        ClusterManager clusterManager = cluster.clusterManager();
        LOGGER.log(Level.INFO, "Connection to Couchbase server successful");

        /*
        // Delete the bucket if it already exists
        if (isDeleteBucketIfExists && clusterManager.hasBucket(bucketName) && (chunkNumber == 1 || chunkNumber == -1)) {
            clusterManager.removeBucket(bucketName);
            LOGGER.log(Level.INFO, bucketName + " bucket deleted");
        }

        // Create the bucket for the feed data
        BucketSettings bucketSettings = new DefaultBucketSettings.Builder().type(BucketType.COUCHBASE).name(bucketName)
                .password("").quota(bucketSize).replicas(1).indexReplicas(true).enableFlush(true).build();

        // Create the bucket if it does not exist
        if (!clusterManager.hasBucket(bucketName)) {
            clusterManager.insertBucket(bucketSettings);
            LOGGER.log(Level.INFO, bucketName + " bucket created");

            // Give the bucket some time to get created before opening it
            Thread.sleep(10);
        }
         */

        // Get the created bucket
        Bucket bucket = cluster.openBucket(bucketName);
        LOGGER.log(Level.INFO, bucketName + " bucket opened");

        // Start time
        long startTime = System.currentTimeMillis();

        // chunNumber -1 will result in generating all chunks on a single partition
        if (chunkNumber == -1) {
            // We start from 1 since chunk numbers start from 1
            for (int i = 1; i <= parallelism; i++) {
                Session session = Session.getDefaultSession().withScale(scalingFactor).withParallelism(parallelism)
                        .withChunkNumber(i);

                // TPC chunks start at 1 not 0, so parallelism + 1 is the chunk number
                TpcdsGeneratorAndLoaderRunnable runnable = new TpcdsGeneratorAndLoaderRunnable(i, session, TABLE_TO_GENERATE, bucket, batchLimit);
                executorService.submit(runnable);
            }
        } else {
            // Generating a single chunk
            Session session = Session.getDefaultSession().withScale(scalingFactor).withParallelism(parallelism)
                    .withChunkNumber(chunkNumber);

            // TPC chunks start at 1 not 0, so parallelism + 1 is the chunk number
            TpcdsGeneratorAndLoaderRunnable runnable = new TpcdsGeneratorAndLoaderRunnable(chunkNumber, session, TABLE_TO_GENERATE, bucket, batchLimit);
            executorService.submit(runnable);
        }

        // Wait for all partitions to finish their work
        executorService.shutdown();
        try {
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
            executorService.shutdownNow();
        } catch (Exception ex) {
            executorService.shutdownNow();
            LOGGER.log(Level.WARN, ex.getMessage());
        }

        // End time
        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime) / 1000.00;
        System.out.println("Total time: " + duration + " seconds");
        LOGGER.log(Level.INFO, "Total time: " + duration + " seconds");
        LOGGER.log(Level.INFO, "Data generation completed");

        // Release all resources
        cluster.disconnect();
    }

    /**
     * Reads the configuration from a properties file, then override it with command line arguments. Any parameters
     * that are not provided will use their default values.
     */
    private static void loadConfiguration(String[] arguments, Map<String, String> configuration) {

        // First, read the arguments from the properties file
        try (InputStream inputStream =
                     TpcdsToKVFeeder.class.getClassLoader().getResourceAsStream(PROPERTIES_FILE_NAME)) {
            Properties properties = new Properties();

            if (inputStream != null) {
                properties.load(inputStream);
                for (Map.Entry<Object, Object> entry : properties.entrySet()) {
                    configuration.put(entry.getKey().toString().toLowerCase(), entry.getValue().toString());
                }
            }
        } catch (FileNotFoundException ex) {
            LOGGER.log(Level.WARN, "Configuration file not found");
        } catch (Exception ex) {
            LOGGER.log(Level.WARN, "Failed to read configuration file");
        }

        // Second, override any properties with anything that is provided in command line arguments
        if (arguments != null && arguments.length > 0) {
            for (String arg : arguments) {
                if (arg.contains("=")) {
                    configuration.put(arg.substring(0, arg.indexOf('=')).toLowerCase(), arg.substring(arg.indexOf('=') + 1));
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
        username = config.get(USER_NAME_FIELD_NAME) != null ? config.get(USER_NAME_FIELD_NAME) : username;
        password = config.get(PASSWORD_FIELD_NAME) != null ? config.get(PASSWORD_FIELD_NAME) : password;
        bucketName = config.get(BUCKET_NAME_FIELD_NAME) != null ? config.get(BUCKET_NAME_FIELD_NAME) : bucketName;
        isDeleteBucketIfExists = config.get(IS_DELETE_IF_BUCKET_EXISTS_FIELD_NAME) != null
                ? Boolean.valueOf(config.get(IS_DELETE_IF_BUCKET_EXISTS_FIELD_NAME)) : isDeleteBucketIfExists;
        bucketSize = config.get(BUCKET_SIZE_FIELD_NAME) != null ? Integer.valueOf(config.get(BUCKET_SIZE_FIELD_NAME))
                : bucketSize;
        batchLimit = config.get(BATCH_LIMIT_FIELD_NAME) != null
                ? Integer.valueOf(config.get(BATCH_LIMIT_FIELD_NAME)) : batchLimit;
        scalingFactor = config.get(SCALING_FACTOR_FIELD_NAME) != null
                ? Double.valueOf(config.get(SCALING_FACTOR_FIELD_NAME)) : scalingFactor;
        parallelism = config.get(PARALLELISM_FIELD_NAME) != null ? Integer.valueOf(config.get(PARALLELISM_FIELD_NAME))
                : parallelism;
        chunkNumber = config.get(CHUNK_NUMBER_FIELD_NAME) != null ? Integer.valueOf(config.get(CHUNK_NUMBER_FIELD_NAME))
                : chunkNumber;
        kvEndpoints = config.get(KV_ENDPOINTS_FIELD_NAME) != null ? Integer.valueOf(config.get(KV_ENDPOINTS_FIELD_NAME))
                : kvEndpoints;
        kvTimeout = config.get(KV_TIMEOUT_FIELD_NAME) != null ? Integer.valueOf(config.get(KV_TIMEOUT_FIELD_NAME))
                : kvTimeout;
        failureRetryDelay = config.get(FAILURE_RETRY_DELAY_FIELD_NAME) != null
                ? Integer.valueOf(config.get(FAILURE_RETRY_DELAY_FIELD_NAME)) : failureRetryDelay;
    }
}
