package org.couchbase;

import com.couchbase.client.java.*;

import java.time.Duration;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This singleton class ensures only one instance of couchbase cluster connection.
 *
 * @author abhijeetbehera
 */
public class DatabaseConfiguration {

	private static final String CONNECTION_STRING = "couchbase://3.83.137.252";// Couchbase Server Link
	private static final String USERNAME = "Administrator";
	private static final String PASSWORD = "6hmwuO2s@WzfMRO6";
	private static final String BUCKET = "SimCardInfo";
	private static final String SCOPE = "_default";
	private static final String COLLECTION = "_default";
	public static final ReactiveCluster REACTIVE_CLUSTER;
	public static final ReactiveBucket REACTIVE_BUCKET;
	public static final ReactiveScope REACTIVE_SCOPE;
	public static final ReactiveCollection REACTIVE_COLLECTION;

	private static volatile DatabaseConfiguration instance;

	private static final Cluster cluster;
	private static final Bucket bucket;
	private static final Scope scope;
	private static final Collection collection;

	static {
		cluster = Cluster.connect(
				CONNECTION_STRING,
				ClusterOptions.clusterOptions(USERNAME, PASSWORD).environment(env -> {
					env.applyProfile("wan-development");
				})
		);

		bucket = cluster.bucket(BUCKET);
		bucket.waitUntilReady(Duration.ofSeconds(10));
		scope = bucket.scope(SCOPE);
		collection = scope.collection(COLLECTION);

		REACTIVE_CLUSTER = cluster.reactive();
		REACTIVE_BUCKET = bucket.reactive();
		REACTIVE_SCOPE = scope.reactive();
		REACTIVE_COLLECTION = collection.reactive();

// Is there a way to disable Java SDK Info & Warn message logs? It clutters the console with info messages
		Logger rootLogger = Logger.getLogger("com.couchbase");
		rootLogger.setLevel(Level.OFF);
	}

	private DatabaseConfiguration() {
		// Private constructor to prevent instantiation from outside
	}

	public static DatabaseConfiguration getInstance() {
		if (instance == null) {
			synchronized (DatabaseConfiguration.class) {
				if (instance == null) {
					instance = new DatabaseConfiguration();
				}
			}
		}
		return instance;
	}

	public ReactiveCluster getCluster() {
		return REACTIVE_CLUSTER;
	}

	public ReactiveBucket getBucket() {
		return REACTIVE_BUCKET;
	}

	public ReactiveScope getScope() {
		return REACTIVE_SCOPE;
	}

	public ReactiveCollection getCollection() {
		return REACTIVE_COLLECTION;
	}
}
