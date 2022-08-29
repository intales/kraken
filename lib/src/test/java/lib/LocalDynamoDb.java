package lib;

import java.net.URI;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;

class LocalDynamoDb {
	private static final int DEFAULT_PORT = 8181;
	private static final long CAPACITY = 10000L;
	private static final String SECRET = "dummy-secret";
	private static final String KEY = "dummy-key";
	private DynamoDBProxyServer server;
	private DynamoDbClient client;

	public LocalDynamoDb() {
		AwsDynamoDbLocalTestUtils.initSqLite();
		client = createClient();
	}

	/**
	 * Start the local DynamoDb service and run in background
	 */
	void start() {
		String portString = Integer.toString(DEFAULT_PORT);

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
	private DynamoDbClient createClient() {
		String endpoint = String.format("http://localhost:%d", DEFAULT_PORT);
		return DynamoDbClient
				.builder()
				.endpointOverride(URI.create(endpoint))
				// The region is meaningless for local DynamoDb but required for client builder
				// validation
				.region(Region.EU_CENTRAL_1)
				.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(KEY, SECRET)))
				.build();
	}

	DynamoDbClient getClient() {
		return client;
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

	private static RuntimeException propagate(Exception e) {
		if (e instanceof RuntimeException) {
			throw (RuntimeException) e;
		}
		throw new RuntimeException(e);
	}

	public String createTable(String tableName, String key) {
		DynamoDbWaiter dbWaiter = client.waiter();
		CreateTableRequest request = CreateTableRequest
				.builder()
				.keySchema(KeySchemaElement.builder().attributeName(key).keyType(KeyType.HASH).build())
				.attributeDefinitions(
						AttributeDefinition.builder().attributeName(key).attributeType(ScalarAttributeType.S).build())
				.tableName(tableName)
				.provisionedThroughput(ProvisionedThroughput
						.builder()
						.readCapacityUnits(CAPACITY)
						.writeCapacityUnits(CAPACITY)
						.build())
				.build();

		String newTable = "";
		try {
			CreateTableResponse response = client.createTable(request);
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

	public boolean insertData(String tableName, Map<String, AttributeValue> itemValues) {
		PutItemRequest request = PutItemRequest.builder().tableName(tableName).item(itemValues).build();
		try {
			client.putItem(request);
			System.out.println(tableName + " was successfully updated");
			return true;
		} catch (ResourceNotFoundException e) {
			System.err.format("Error: The Amazon DynamoDB table \"%s\" can't be found.\n", tableName);
			System.err.println("Be sure that it exists and that you've typed its name correctly!");
		} catch (DynamoDbException e) {
			System.err.println(e.getMessage());
		}
		return false;
	}

	public List<Map<String, AttributeValue>> scanTable(String tableName) {
		ScanRequest request = ScanRequest.builder().tableName(tableName).build();
		ScanResponse response = client.scan(request);
		return response.items();
	}
}
