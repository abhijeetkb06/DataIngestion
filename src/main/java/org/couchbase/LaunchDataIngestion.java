package org.couchbase;

import com.couchbase.client.java.json.JsonObject;

import java.util.List;
import java.util.concurrent.*;
import java.util.stream.IntStream;

/**
 * Initiates the data ingestion process into Couchbase.
 */
public class LaunchDataIngestion {

	public static void main(String[] args) {

		// BlockingQueue shared between producers and consumers
		BlockingQueue<List<JsonObject>> sharedTasksQueue = new LinkedBlockingQueue<>(1000); // Bounded queue to avoid memory issues

		// Executor service for managing producer and consumer threads
		ExecutorService executorService = Executors.newFixedThreadPool(ConcurrencyConfig.EXECUTOR_THREAD_POOL);

		// Start producers
		IntStream.range(ConcurrencyConfig.PRODUCER_START_RANGE, ConcurrencyConfig.PRODUCER_END_RANGE)
				.forEach(i -> executorService.execute(new DataProducer(sharedTasksQueue)));

		// Start consumers
		IntStream.range(ConcurrencyConfig.CONSUMER_START_RANGE, ConcurrencyConfig.CONSUMER_END_RANGE)
				.forEach(i -> executorService.execute(new DataConsumer(sharedTasksQueue)));

		// Optionally, schedule a task to forcefully terminate the application (remove if not needed)
        /*Executors.newSingleThreadScheduledExecutor().schedule(() -> {
            System.out.println("Forcefully terminating the application...");
            System.exit(130); // Exit with a specific status code
        }, 10, TimeUnit.MINUTES);*/  // Adjust or remove based on usage
	}
}
