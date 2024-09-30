package org.couchbase;


import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import net.datafaker.Faker;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
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
                .map(i -> generateMockSimCardData(i))
                .doOnError(e -> Flux.empty())
                .sequential()
                .collectList()
                .block();
    }

    private static JsonObject generateMockSimCardData(int index) {
        Random random = new Random();
        Faker faker = new Faker();  // Create an instance of Faker

        JsonObject jsonData = JsonObject.from(new LinkedHashMap<>());

        // Provider Info
        JsonObject providerInfo = JsonObject.from(new LinkedHashMap<>());
        providerInfo.put("providerName", "Verizon");
        providerInfo.put("providerType", "Mobile");
        providerInfo.put("provisionedAt", Instant.now().toString());
        providerInfo.put("status", random.nextBoolean() ? "Active" : "Inactive");

        // Customer Info (Use Faker to generate dynamic customer info)
        JsonObject customerInfo = JsonObject.from(new LinkedHashMap<>());
        customerInfo.put("customerId", "CUST" + random.nextInt(1000000));
        customerInfo.put("firstName", faker.name().firstName());  // Dynamic first name
        customerInfo.put("lastName", faker.name().lastName());    // Dynamic last name
        customerInfo.put("email", faker.internet().emailAddress()); // Dynamic email
        customerInfo.put("phone", faker.phoneNumber().cellPhone());  // Dynamic phone number

        // Use Faker to generate dynamic address
        JsonObject address = JsonObject.from(new LinkedHashMap<>());
        address.put("street", faker.address().streetAddress());
        address.put("city", faker.address().city());
        address.put("state", faker.address().stateAbbr());
        address.put("zipCode", faker.address().zipCode());
        customerInfo.put("address", address);

        // Generate multiple devices, each with unique SIM card information
        List<JsonObject> devices = new ArrayList<>();
        int deviceCount = random.nextInt(4) + 1; // Each customer will have 1 to 4 devices
        for (int i = 0; i < deviceCount; i++) {
            JsonObject deviceInfo = JsonObject.from(new LinkedHashMap<>());

            // Device Info (Use UUID for deviceId, Faker for device model and type)
            deviceInfo.put("deviceId", UUID.randomUUID().toString());
            deviceInfo.put("deviceType", random.nextBoolean() ? "Smartphone" : "Tablet");
            deviceInfo.put("manufacturer", random.nextBoolean() ? "Apple" : "Samsung");
            deviceInfo.put("model", random.nextBoolean() ? "iPhone " + faker.number().numberBetween(10, 15) : "Galaxy S" + faker.number().numberBetween(20, 23));
            deviceInfo.put("osVersion", random.nextBoolean() ? "iOS " + faker.number().numberBetween(14, 18) : "Android " + faker.number().numberBetween(10, 13));
            deviceInfo.put("lastSyncTime", Instant.now().minusSeconds(random.nextInt(86400)).toString());

            // SIM Card Details (Each device has unique SIM information)
            JsonObject simCardDetails = JsonObject.from(new LinkedHashMap<>());
            simCardDetails.put("simId", "899110" + nextLong(random, 1000000000L));  // Dynamic SIM ID
            simCardDetails.put("iccid", "8914800000925" + (100000000L + random.nextInt(99999999)));
            simCardDetails.put("imsi", "310410" + (100000000L + random.nextInt(99999999)));
            simCardDetails.put("msisdn", faker.phoneNumber().cellPhone());  // Dynamic phone number
            simCardDetails.put("activationDate", LocalDate.now().minusDays(random.nextInt(30)).toString());
            simCardDetails.put("expirationDate", LocalDate.now().plusYears(2).toString());

            // Add SIM card details under the device info
            deviceInfo.put("simCardDetails", simCardDetails);
            devices.add(deviceInfo);
        }

        // Service Details (Common across all devices for the customer)
        JsonObject serviceDetails = JsonObject.from(new LinkedHashMap<>());
        serviceDetails.put("planName", random.nextBoolean() ? "Unlimited Plus" : "Basic 5GB Plan");
        serviceDetails.put("dataLimit", random.nextBoolean() ? "Unlimited" : "5GB");
        serviceDetails.put("currentDataUsage", random.nextInt(50) + "GB");
        serviceDetails.put("renewalDate", LocalDate.now().plusDays(random.nextInt(30)).toString());
        serviceDetails.put("internationalRoaming", random.nextBoolean());
        serviceDetails.put("voiceMail", random.nextBoolean());
        serviceDetails.put("smsLimit", "Unlimited");

        // Network Info (Could also vary per device, but we'll keep it common here)
        JsonObject networkInfo = JsonObject.from(new LinkedHashMap<>());
        networkInfo.put("networkType", random.nextBoolean() ? "5G" : "4G");
        networkInfo.put("homeNetwork", "Verizon");
        networkInfo.put("currentNetwork", random.nextBoolean() ? "Verizon" : "Roaming Network");
        networkInfo.put("roamingStatus", random.nextBoolean());
        networkInfo.put("signalStrength", random.nextBoolean() ? "Good" : "Weak");
        networkInfo.put("lastNetworkSwitch", Instant.now().minusSeconds(random.nextInt(86400)).toString());

        // Billing Info (Common for all devices under one customer)
        JsonObject billingInfo = JsonObject.from(new LinkedHashMap<>());
        billingInfo.put("billingCycleStart", LocalDate.now().minusDays(random.nextInt(30)).toString());
        billingInfo.put("billingCycleEnd", LocalDate.now().plusDays(30).toString());
        billingInfo.put("currentCharges", faker.commerce().price(50.0, 150.0)); // Dynamic charges
        billingInfo.put("paymentDueDate", LocalDate.now().plusDays(random.nextInt(10)).toString());
        billingInfo.put("autoPayEnabled", random.nextBoolean());

        // Events (Can be tied to customer, not per device, for simplicity)
        List<JsonObject> events = new ArrayList<>();
        for (int i = 0; i < random.nextInt(3) + 1; i++) {
            JsonObject event = JsonObject.from(new LinkedHashMap<>());
            event.put("eventId", "EVT" + random.nextInt(1000000));
            event.put("eventType", random.nextBoolean() ? "SIM Activation" : "Data Usage Alert");
            event.put("eventTime", Instant.now().minusSeconds(random.nextInt(86400)).toString());
            event.put("eventDescription", random.nextBoolean() ? "SIM card activated" : "Data usage reached limit");
            events.add(event);
        }

        // Assemble the final JSON data
        jsonData.put("providerInfo", providerInfo);
        jsonData.put("deviceInfo", devices); // Add the list of devices with unique SIM info
        jsonData.put("customerInfo", customerInfo);
        jsonData.put("serviceDetails", serviceDetails);
        jsonData.put("networkInfo", networkInfo);
        jsonData.put("billingInfo", billingInfo);
        jsonData.put("events", events);

        return jsonData;
    }

    // Custom nextLong method to generate a random long value within a bound
    private static long nextLong(Random random, long bound) {
        return (long) (random.nextDouble() * bound);
    }


}