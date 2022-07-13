package dynamodb;

import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import config.Configuration;
import main.DataManager;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public class DynamoDB implements DataManager {
	Configuration configuration;
	Region region;
	AwsCredentialsProvider credentialsProvider;
	DynamoDbClient client;
	ConcurrentHashMap<Interaction, InteractionData> interactionMap;
	Vector<Integer> counterVector;

 
	public DynamoDB(Configuration configuration) {
		this(configuration, Region.EU_CENTRAL_1);
	}
	
	public DynamoDB(Configuration configuration, Region region) {
		this.region = region;
		credentialsProvider = ProfileCredentialsProvider.create();
		
		interactionMap = new ConcurrentHashMap<>();
		counterVector = new Vector<>();
		
		client = DynamoDbClient.builder()
				.credentialsProvider(credentialsProvider)
				.region(region).build();
		
		
	}

	@Override
	public void scan() {
		// TODO Auto-generated method stub

	}

	@Override
	public void update() {
		// TODO Auto-generated method stub

	}

}
