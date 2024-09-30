package org.couchbase;

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.java.json.JsonObject;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * DataConsumer class for bulk data insertion using reactive Couchbase APIs.
 *
 * Enhanced error logging and retry mechanism.
 *
 * @author abhijeet
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
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// Bulk insert a batch of JSON documents into Couchbase
	private void bulkInsertBatch(List<JsonObject> dataBatch) {
		DatabaseConfiguration dbConfig = DatabaseConfiguration.getInstance();

		Flux.fromIterable(dataBatch)
				.parallel(Math.min(ConcurrencyConfig.BULK_INSERT_CONCURRENT_OPS, 10))  // Reduce concurrency to avoid overwhelming the cluster
				.runOn(Schedulers.boundedElastic()) // Suitable for I/O operations
				.flatMap(doc -> dbConfig.getCollection()
						.upsert(doc.getString("id"), doc) // Using 'id' field as the key for upsert
						.retryWhen(Retry.backoff(3, Duration.ofMillis(500)))  // Retry 3 times with exponential backoff
						.onErrorResume(e -> {
							// Log the detailed error message
							if (e instanceof CouchbaseException) {
								CouchbaseException cbEx = (CouchbaseException) e;
								System.err.println("Error inserting document: " + doc.getString("id") + ". Reason: " + cbEx.getMessage());
							} else {
								System.err.println("Unexpected error inserting document: " + doc.getString("id") + ". Reason: " + e.getMessage());
							}
							return Mono.empty();  // Skip failed documents
						})
				)
				.sequential()
				.collectList()
				.block();  // Ensure all operations complete before proceeding
	}
}
