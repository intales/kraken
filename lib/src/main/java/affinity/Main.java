package affinity;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;

public class Main {

	public Main() {
	}

	public static void v1() {
		String region = "eu-central-1";
		String tableName = "Interaction-lgwgf74xczh3jddacdgxrh7smy-test";

		com.amazonaws.auth.profile.ProfileCredentialsProvider credential = new com.amazonaws.auth.profile.ProfileCredentialsProvider(
				"development");

		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
				.withRegion(region).withCredentials(credential)
				.build();
		DynamoDB dynamoDB = new DynamoDB(client);
		Table table = dynamoDB.getTable(tableName);

		ScanSpec spec = new ScanSpec();// .withMaxResultSize(itemLimit).withTotalSegments(totalSegments).withSegment(segment);

		table.scan(spec).forEach(t -> {
			System.out.println("likes: " + t.get("likes")
					+ "\tcomments: " + t.get("comments")
					+ "\tcollabotations: " + t.get("collaborations"));
		});
	}

	public static void v2() {
		Region region = Region.EU_CENTRAL_1;// "eu-central-1";
		String tableName = "Interaction-lgwgf74xczh3jddacdgxrh7smy-test";

		AwsCredentialsProvider credentialsProvider = ProfileCredentialsProvider
				.create("development");

		ScanRequest scanRequest = ScanRequest.builder()
				.tableName(tableName).build();

		DynamoDbClient ddb = DynamoDbClient.builder()
				.credentialsProvider(credentialsProvider)
				.region(region).build();

		for (Map<String, AttributeValue> item : ddb.scan(scanRequest)
				.items()) {
			System.out.println("likes: " + item.get("likes").n()
					+ "\tcomments: " + item.get("comments").n()
					+ "\tcollaborations: "
					+ item.get("collaborations").n());
		}
	}

	public static void main(String[] args) {
		Instant start = Instant.now();
		v1();
		Instant end = Instant.now();
		System.out.println(
				"V1: " + Duration.between(start, end).toMillis());

		start = Instant.now();
		v2();
		end = Instant.now();
		System.out.println(
				"V2: " + Duration.between(start, end).toMillis());
	}

}
