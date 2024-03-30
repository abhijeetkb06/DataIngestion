package org.couchbase;

import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.MutationResult;
import reactor.core.publisher.Flux;
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
				
				System.out.println("***************QUEUE SIZE************** "+ tasksQueue.size());

				// Remove the user from shared queue and process
				bulkInsert(tasksQueue.take());

//				bulkInsert(generateMockDataParallel());
				System.out.println(" CONSUMED \n");
				System.out.println(" Thread Name: " + Thread.currentThread().getName());
//				Thread.sleep(100);
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
				.flatMap(doc -> dbConfig.getCollection().upsert(doc.getString("transactionId"),doc))
		        .doOnError(e -> Flux.empty())
				.sequential()
				.collectList()
				.block();
	}
}
