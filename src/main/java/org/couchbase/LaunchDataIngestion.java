package org.couchbase;

import com.couchbase.client.java.json.JsonObject;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
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
	}
}
