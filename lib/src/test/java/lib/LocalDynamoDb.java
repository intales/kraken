package lib;

import java.net.URI;

import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;

class LocalDynamoDb {
	private static final long CAPACITY = 10000L;
	private static final String SECRET = "dummy-secret";
	private static final String KEY = "dummy-key";
	private DynamoDBProxyServer server;
	private int port;

	public LocalDynamoDb() {
		AwsDynamoDbLocalTestUtils.initSqLite();
	}

	/**
	 * Start the local DynamoDb service and run in background
	 */
	void start() {
		port = getFreePort();
		String portString = Integer.toString(port);

		try {
			server = createServer(portString);
			server.start();
		} catch (Exception e) {
			throw propagate(e);
		}
	}

	/**
	 * Create a standard AWS v2 SDK client pointing to the local DynamoDb instance
	 * 
	 * @return A DynamoDbClient pointing to the local DynamoDb instance
	 */
	DynamoDbClient createClient() {
		String endpoint = String.format("http://localhost:%d", port);
		return DynamoDbClient
				.builder()
				.endpointOverride(URI.create(endpoint))
				// The region is meaningless for local DynamoDb but required for client builder
				// validation
				.region(Region.EU_CENTRAL_1)
				.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(KEY, SECRET)))
				.build();
	}

	/**
	 * Stops the local DynamoDb service and frees up resources it is using.
	 */
	void stop() {
		try {
			server.stop();
		} catch (Exception e) {
			throw propagate(e);
		}
	}

	private DynamoDBProxyServer createServer(String portString) throws Exception {
		return ServerRunner.createServerFromCommandLineArgs(new String[] { "-inMemory", "-port", portString });
	}

	private int getFreePort() {
		return 8080;
		/*
		 * try { ServerSocket socket = new ServerSocket(0); int port =
		 * socket.getLocalPort(); socket.close(); return port; } catch (IOException ioe)
		 * { throw propagate(ioe); }
		 */
	}

	private static RuntimeException propagate(Exception e) {
		if (e instanceof RuntimeException) {
			throw (RuntimeException) e;
		}
		throw new RuntimeException(e);
	}

	public String createTable(String tableName, String key) {
		DynamoDbClient ddb = createClient();
		DynamoDbWaiter dbWaiter = ddb.waiter();
		CreateTableRequest request = CreateTableRequest
				.builder()
				.attributeDefinitions(
						AttributeDefinition.builder().attributeName(key).attributeType(ScalarAttributeType.S).build())
				.keySchema(KeySchemaElement.builder().attributeName(key).keyType(KeyType.HASH).build())
				.tableName(tableName)
				.provisionedThroughput(ProvisionedThroughput
						.builder()
						.readCapacityUnits(CAPACITY)
						.writeCapacityUnits(CAPACITY)
						.build())
				.build();

		String newTable = "";
		try {
			CreateTableResponse response = ddb.createTable(request);
			DescribeTableRequest tableRequest = DescribeTableRequest.builder().tableName(tableName).build();

			// Wait until the Amazon DynamoDB table is created
			WaiterResponse<DescribeTableResponse> waiterResponse = dbWaiter.waitUntilTableExists(tableRequest);
			waiterResponse.matched().response().ifPresent(System.out::println);

			newTable = response.tableDescription().tableName();
			return newTable;
		} catch (DynamoDbException e) {
			propagate(e);
			return "";
		}
	}
}