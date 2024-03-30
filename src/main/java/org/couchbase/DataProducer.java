package org.couchbase;


import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;

/**
 * This initiates the process of bulk mock data generation in Couchbase using reactive apis.
 *
 * @author abhijeetbehera
 */
public class DataProducer extends Thread {

    // Load data in queue
    private BlockingQueue<List<JsonObject>> sharedQueue;

    public DataProducer(BlockingQueue<List<JsonObject>> sharedQueue) {
        super("PRODUCER");
        this.sharedQueue = sharedQueue;
    }

    public void run() {

        while (true) {
            try {
                List<JsonObject> mockDataList = generateMockDataParallel();

                // Add list of mock data to shared queue.
                sharedQueue.put(mockDataList);

                System.out.println("@@@@@@@@@ PRODUCED @@@@@@@@ " + sharedQueue.size());
                System.out.println(" Thread Name: " + Thread.currentThread().getName());

                Thread.sleep(50);
            } catch (InterruptedException e) {
            } catch (CouchbaseException ex) {
                System.err.println("Something else happened: " + ex);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static List<JsonObject> generateMockDataParallel() {

        return Flux.range(ConcurrencyConfig.MOCK_DATA_START_RANGE, ConcurrencyConfig.MOCK_DATA_END_RANGE)
                .parallel(ConcurrencyConfig.MOCK_DATA_PARALLELISM)
                .runOn(Schedulers.parallel())
                .map(i -> generateMockTransactionsData(i))
                .doOnError(e -> Flux.empty())
                .sequential()
                .collectList()
                .block();
    }

    private static JsonObject generateMockTransactionsData(int index) {
        Random random = new Random();
        String[] transactionTypes = {"credit card payment", "loan payment", "deposit", "withdrawal", "investment", "fee"};

        JsonObject jsonData = JsonObject.from(new LinkedHashMap<>());

        jsonData.put("transactionId", UUID.randomUUID().toString());
        jsonData.put("accountId", "ACC" + random.nextInt(1000));
        jsonData.put("type", transactionTypes[random.nextInt(transactionTypes.length)]); // Dynamically selected transaction type
        jsonData.put("amount", 100 + random.nextDouble() * 500); // Example transaction amount
        jsonData.put("currency", "USD");
        jsonData.put("timestamp", Instant.now().toString());

        JsonObject details = JsonObject.from(new LinkedHashMap<>());
        if (jsonData.get("type").equals("loan payment")) {
            details.put("loanId", "LN" + random.nextInt(1000));
            details.put("paymentType", random.nextBoolean() ? "principal and interest" : "interest only");
        } else if (jsonData.get("type").equals("credit card payment")) {
            details.put("cardNumber", "**** **** **** " + (1000 + random.nextInt(8999)));
            details.put("paymentDueDate", LocalDate.now().plusDays(random.nextInt(10) + 1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
        }
        // Additional conditionals can be added here for other transaction types as needed

        // Generate a dynamic due date for transactions that require it
        LocalDate dueDate = LocalDate.now().plusDays(random.nextInt(30) + 1);
        details.put("dueDate", dueDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));

        details.put("paymentMethod", random.nextBoolean() ? "automatic bank draft" : "online transfer");
        if (jsonData.get("type").equals("loan payment")) {
            details.put("loanBalanceBefore", 5000 + random.nextDouble() * 5000);
            details.put("loanBalanceAfter", details.getDouble("loanBalanceBefore") - jsonData.getDouble("amount"));
        }
        jsonData.put("details", details);

        jsonData.put("status", random.nextBoolean() ? "completed" : "pending");

        return jsonData;
    }
}