package org.couchbase;

import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.MutationResult;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * This initiates the process of bulk data insert in Couchbase using reactive apis.
 *
 * @author abhijeetbehera
 */
public class DataConsumer extends Thread {

	// Read data to consume once data is loaded in queue
	private BlockingQueue<List<JsonObject>> tasksQueue;

	public DataConsumer(BlockingQueue<List<JsonObject>> tasksQueue) {
		super("CONSUMER");
		this.tasksQueue = tasksQueue;
	}

	public void run() {
		try {
			while (true) {

				System.out.println("***************QUEUE SIZE************** " + tasksQueue.size());

				// Remove the data from the shared queue and process
				List<JsonObject> dataBatch = tasksQueue.take();
				bulkInsert(dataBatch);

				System.out.println(" CONSUMED \n");
				System.out.println(" Thread Name: " + Thread.currentThread().getName());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private List<MutationResult> bulkInsert(List<JsonObject> data) {
		DatabaseConfiguration dbConfig = DatabaseConfiguration.getInstance();
		return Flux.fromIterable(data)
				.parallel(ConcurrencyConfig.BULK_INSERT_CONCURRENT_OPS)
				.runOn(Schedulers.boundedElastic()) // or one of your choice
				.flatMap(doc -> {

					// Fetch the array of devices and process them safely
					JsonArray devicesArray = doc.getArray("deviceInfo");

					if (devicesArray != null) {
						return Flux.fromIterable(devicesArray)
								.flatMap(deviceObj -> {
									JsonObject device = (JsonObject) deviceObj; // Explicit cast to JsonObject

									// Use the deviceId as the key for insertion
									return dbConfig.getCollection()
											.upsert(device.getString("deviceId"), doc)
											.onErrorResume(e -> {
												System.err.println("Error inserting device: " + e.getMessage());
												return Mono.empty(); // Use Mono.empty() instead of Flux.empty()
											});
								});
					} else {
						// If no devices found, return an empty flux
						return Flux.empty();
					}
				})
				.doOnError(e -> Flux.empty())
				.sequential()
				.collectList()
				.block();
	}
}

