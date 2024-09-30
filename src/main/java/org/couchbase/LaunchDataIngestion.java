package org.couchbase;

import com.couchbase.client.java.json.JsonObject;

import java.util.List;
import java.util.concurrent.*;
import java.util.stream.IntStream;

/**
 * This initiates the process of data ingestion in Couchbase.
 * 
 * @author abhijeetbehera
 */
public class LaunchDataIngestion {

	public static void main(String[] args) {

		BlockingQueue<List<JsonObject>> sharedTasksQueue = new LinkedBlockingQueue<List<JsonObject>>();

		ExecutorService executorService = Executors.newFixedThreadPool(ConcurrencyConfig.EXECUTOR_THREAD_POOL);

		IntStream.range(ConcurrencyConfig.PRODUCER_START_RANGE, ConcurrencyConfig.PRODUCER_END_RANGE)
				.forEach(i -> {
					executorService.execute(new DataProducer(sharedTasksQueue));
				});

		IntStream.range(ConcurrencyConfig.CONSUMER_START_RANGE, ConcurrencyConfig.CONSUMER_END_RANGE)
				.forEach(i -> {
					executorService.execute(new DataConsumer(sharedTasksQueue));
				});

		// Schedule a task to forcefully terminate the application after 1 minutes to limit the data load
		// TODO: Remove this code if you want to run the application indefinitely
		/*Executors.newSingleThreadScheduledExecutor().schedule(() -> {
			System.out.println("Forcefully terminating the application...");
			System.exit(130); // Exit with a specific status code (130 is commonly used for SIGINT)
		}, 1, TimeUnit.MINUTES);*/
	}
}
