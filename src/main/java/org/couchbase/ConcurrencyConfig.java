package org.couchbase;

/**
 * This defines the concurrency parameters for executor thread pool, producer, consumer, mock data generation and bulk insert parallelism.
 *
 * @author abhijeetbehera
 */
public class ConcurrencyConfig {

    // Executor thread pool config and producer consumer thread range
    public static final int EXECUTOR_THREAD_POOL = 40;
    public static final int PRODUCER_START_RANGE = 0;
    public static final int PRODUCER_END_RANGE = 20;
    public static final int CONSUMER_START_RANGE = 0;
    public static final int CONSUMER_END_RANGE = 20;

    // Mock data generation parallelism
    public static final int MOCK_DATA_START_RANGE = 1;
    public static final int MOCK_DATA_END_RANGE = 20;
    public static final int MOCK_DATA_PARALLELISM = 20;

    // Bulk insert parallelism
    public static final int BULK_INSERT_CONCURRENT_OPS = 20;
}
