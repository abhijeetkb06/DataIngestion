package org.couchbase;

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.json.JsonArray;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * DataConsumer class for bulk data insertion using reactive Couchbase APIs.
 */
public class DataConsumer extends Thread {

	private final BlockingQueue<List<JsonObject>> tasksQueue;

	public DataConsumer(BlockingQueue<List<JsonObject>> tasksQueue) {
		super("CONSUMER");
		this.tasksQueue = tasksQueue;
	}

	public void run() {
		try {
			while (true) {
				// Fetch a batch of documents from the queue
				List<JsonObject> dataBatch = tasksQueue.take();

				// Process the batch
				bulkInsertBatch(dataBatch);

				System.out.println("Processed batch of size: " + dataBatch.size() + ", Remaining Queue size: " + tasksQueue.size());
			}
		} catch (InterruptedException e) {
			System.err.println("Consumer interrupted: " + e.getMessage());
			Thread.currentThread().interrupt();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// Bulk insert a batch of JSON documents into Couchbase
	private void bulkInsertBatch(List<JsonObject> dataBatch) {
		DatabaseConfiguration dbConfig = DatabaseConfiguration.getInstance();

		// Stream through the dataBatch using Flux
		Flux.fromIterable(dataBatch)
				.parallel(Math.min(ConcurrencyConfig.BULK_INSERT_CONCURRENT_OPS, 10))  // Control concurrency
				.runOn(Schedulers.boundedElastic()) // Use bounded elastic scheduler
				.flatMap(doc -> {

					// Fetch the array of devices and process them safely
					JsonArray devicesArray = doc.getArray("deviceInfo");

					if (devicesArray != null) {
						// Iterate over each device in the deviceInfo array
						return Flux.fromIterable(devicesArray)
								.flatMap(deviceObj -> {
									JsonObject device = (JsonObject) deviceObj;  // Explicit cast to JsonObject

									// Use the deviceId as the key for insertion
									String deviceId = device.getString("deviceId");

									if (deviceId == null || deviceId.isEmpty()) {
										System.err.println("Skipping device with missing 'deviceId' field: " + device);
										return Mono.empty();
									}

									return dbConfig.getCollection()
											.upsert(deviceId, doc)  // Use 'deviceId' as the key for upsert
											.retryWhen(Retry.backoff(3, Duration.ofMillis(500))  // Retry 3 times with exponential backoff
													.doBeforeRetry(retrySignal -> {
														System.err.println("Retrying insert for deviceId: " + deviceId + " (attempt " + (retrySignal.totalRetries() + 1) + ")");
													})
											)
											.onErrorResume(e -> {
												// Handle Couchbase-specific errors and log more detailed info
												if (e instanceof CouchbaseException) {
													System.err.println("Error inserting device: " + deviceId + ". Couchbase Error: " + ((CouchbaseException) e).getMessage());
												} else {
													System.err.println("Unexpected error inserting device: " + deviceId + ". Reason: " + e.getMessage());
												}
												return Mono.empty();
											});
								});
					} else {
						// If no devices found, return an empty flux
						return Flux.empty();
					}
				})
				.sequential()  // Merge back to sequential after parallel processing
				.collectList() // Collect the results into a List<MutationResult>
				.block();  // Block until all operations complete
	}
}
