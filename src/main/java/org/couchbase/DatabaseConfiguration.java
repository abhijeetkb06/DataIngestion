package org.couchbase;

import com.couchbase.client.java.*;

import java.time.Duration;

/**
 * This singleton class ensures only one instance of couchbase cluster connection.
 *
 * @author abhijeetbehera
 */
public class DatabaseConfiguration {

	private static final String CONNECTION_STRING = "couchbases://cb.kswqdhulqum6iys.cloud.couchbase.com";
	private static final String USERNAME = "abhijeet";
	private static final String PASSWORD = "Password@P1";
	private static final String BUCKET = "tpsdata";
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
