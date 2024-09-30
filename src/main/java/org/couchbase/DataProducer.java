package org.couchbase;

import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import net.datafaker.Faker;
import java.time.Instant;
import java.time.LocalDate;

/**
 * DataProducer generates data in batches for efficient consumption.
 *
 * @author abhijeet
 */
public class DataProducer extends Thread {

    private final BlockingQueue<List<JsonObject>> sharedQueue;
    private static final int BATCH_SIZE = 100; // Define the batch size

    public DataProducer(BlockingQueue<List<JsonObject>> sharedQueue) {
        super("PRODUCER");
        this.sharedQueue = sharedQueue;
    }

    public void run() {
        try {
            while (true) {
                // Generate a batch of mock data
                List<JsonObject> mockDataList = generateMockDataBatch();

                // Add the batch to the shared queue
                sharedQueue.put(mockDataList);

                System.out.println("Produced batch of size: " + mockDataList.size() + ", Queue size: " + sharedQueue.size());

                // Dynamic throttling based on queue size
                if (sharedQueue.size() > 500) {
                    Thread.sleep(200);  // Slow down producers if the queue is overloaded
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Generate a batch of mock data
    private List<JsonObject> generateMockDataBatch() {
        return Flux.range(1, BATCH_SIZE)
                .parallel(ConcurrencyConfig.MOCK_DATA_PARALLELISM)
                .runOn(Schedulers.parallel())
                .map(this::generateMockSimCardData)
                .sequential()
                .collectList()
                .block();  // Generate batch of mock data
    }

    // Data generation logic for a single JSON document
    private JsonObject generateMockSimCardData(int index) {
        Random random = new Random();
        Faker faker = new Faker();  // Use Faker for realistic data

        JsonObject jsonData = JsonObject.create();

        // Add a unique document ID at the root level (important for Couchbase)
        jsonData.put("id", UUID.randomUUID().toString());

        // Provider Info
        JsonObject providerInfo = JsonObject.create();
        providerInfo.put("providerName", "Verizon");
        providerInfo.put("providerType", "Mobile");
        providerInfo.put("provisionedAt", Instant.now().toString());
        providerInfo.put("status", random.nextBoolean() ? "Active" : "Inactive");

        // Customer Info
        JsonObject customerInfo = JsonObject.create();
        customerInfo.put("customerId", "CUST-" + random.nextInt(1000000));
        customerInfo.put("firstName", faker.name().firstName());
        customerInfo.put("lastName", faker.name().lastName());
        customerInfo.put("email", faker.internet().emailAddress());
        customerInfo.put("phone", faker.phoneNumber().cellPhone());

        // Customer Address
        JsonObject address = JsonObject.create();
        address.put("street", faker.address().streetAddress());
        address.put("city", faker.address().city());
        address.put("state", faker.address().stateAbbr());
        address.put("zipCode", faker.address().zipCode());
        customerInfo.put("address", address);

        // Device Info
        JsonArray devicesArray = JsonArray.create();
        int deviceCount = random.nextInt(3) + 1; // Each customer has 1-3 devices
        for (int i = 0; i < deviceCount; i++) {
            JsonObject deviceInfo = JsonObject.create();
            deviceInfo.put("deviceId", UUID.randomUUID().toString());
            deviceInfo.put("deviceType", random.nextBoolean() ? "Smartphone" : "Tablet");
            deviceInfo.put("manufacturer", random.nextBoolean() ? "Apple" : "Samsung");
            deviceInfo.put("model", random.nextBoolean() ? "iPhone " + faker.number().numberBetween(10, 14) : "Galaxy S" + faker.number().numberBetween(20, 23));
            deviceInfo.put("osVersion", random.nextBoolean() ? "iOS " + faker.number().numberBetween(13, 16) : "Android " + faker.number().numberBetween(10, 12));
            deviceInfo.put("lastSyncTime", Instant.now().minusSeconds(random.nextInt(86400)).toString());

            // SIM Card Details
            JsonObject simCardDetails = JsonObject.create();
            simCardDetails.put("simId", "SIM-" + random.nextInt(1000000));
            simCardDetails.put("iccid", "8914800000925" + (100000000L + random.nextInt(99999999)));
            simCardDetails.put("imsi", "310410" + (100000000L + random.nextInt(99999999)));
            simCardDetails.put("msisdn", faker.phoneNumber().cellPhone());
            simCardDetails.put("activationDate", LocalDate.now().minusDays(random.nextInt(30)).toString());
            simCardDetails.put("expirationDate", LocalDate.now().plusYears(2).toString());

            deviceInfo.put("simCardDetails", simCardDetails);

            // Add device to the devices array
            devicesArray.add(deviceInfo);
        }

        // Assemble the final JSON object
        jsonData.put("providerInfo", providerInfo);
        jsonData.put("customerInfo", customerInfo);
        jsonData.put("deviceInfo", devicesArray);

        return jsonData;
    }
}
