package org.couchbase;

/**
 * Defines concurrency parameters for thread pools, data generation, and bulk insert parallelism.
 *
 * @author abhijeet
 */
public class ConcurrencyConfig {

    // Thread pool configuration for producers and consumers
    public static final int EXECUTOR_THREAD_POOL = 30;  // Adjust based on your system's CPU cores
    public static final int PRODUCER_START_RANGE = 0;
    public static final int PRODUCER_END_RANGE = 5;  // Number of producers
    public static final int CONSUMER_START_RANGE = 10;
    public static final int CONSUMER_END_RANGE = 20;  // Number of consumers

    // Mock data generation parallelism
    public static final int MOCK_DATA_PARALLELISM = 10;

    // Bulk insert parallelism
    public static final int BULK_INSERT_CONCURRENT_OPS = 10;
}
