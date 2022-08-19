package aws_lambda;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import config.Configuration;
import config.YAML;
import dynamodb.DynamoDB;
import main.DataManager;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

public class Handler implements RequestHandler<Map<String, String>, String> {
	// You can add initialization code outside of your handler method to reuse
	// resources across multiple invocations. When the runtime loads your handler,
	// it runs static code and the class constructor. Resources that are created
	// during initialization stay in memory between invocations, and can be reused
	// by the handler thousands of times.
	SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
	Region region = Region.EU_CENTRAL_1;

	// init DynamoDB here and reuse
	// download config file from S3 in handleRequest

	private Calendar getDate() {
		Date date = new Date();
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.set(Calendar.MILLISECOND, 0);
		calendar.set(Calendar.SECOND, 0);
		return calendar;
	}

	private String getTodayDate(int minute, int hour) {
		Calendar calendar = getDate();
		calendar.set(Calendar.MINUTE, minute);
		calendar.set(Calendar.HOUR_OF_DAY, hour);
		return formatter.format(calendar.getTime());
	}

	private String getYesterdayDate(int minute, int hour) {
		Calendar calendar = getDate();
		calendar.set(Calendar.MINUTE, minute);
		calendar.set(Calendar.HOUR_OF_DAY, hour);
		calendar.add(Calendar.DATE, -1);
		return formatter.format(calendar.getTime());
	}

	@Override
	public String handleRequest(Map<String, String> input, Context context) {
		String startDate = getYesterdayDate(00, 00);
		String endDate = getTodayDate(00, 00);
		// get config from S3
		System.out.println("Start date = " + startDate);
		System.out.println("End date   = " + endDate);
		Configuration configuration = getConfig();
		if (configuration == null)
			System.exit(1);
		boolean dryRun = false;
		// init datamanager
		DataManager datamanager = new DynamoDB(configuration, dryRun, startDate, endDate);
		datamanager.scan();
		datamanager.update();
		return null;
	}

	private Configuration getConfig() {
		S3Client s3 = S3Client.builder().build();
		// check if file is present
		String path = "/tmp/s3config.yaml";
		try {
			return YAML.getConfiguration(new File(path));
		} catch (FileNotFoundException e) {
			System.err.println("Not found, download");
			// not found, download form s3
			String bucketName = "kraken-configuration";
			String keyName = "source.yaml";

			GetObjectRequest objectRequest = GetObjectRequest.builder().key(keyName).bucket(bucketName).build();

			ResponseBytes<GetObjectResponse> objectBytes = s3.getObjectAsBytes(objectRequest);
			byte[] data = objectBytes.asByteArray();

			// Write the data to a local file.
			File myFile = new File(path);
			OutputStream os;
			try {
				os = new FileOutputStream(myFile);
				os.write(data);
				os.close();
				return YAML.getConfiguration(myFile);
			} catch (IOException ioError) {
				ioError.printStackTrace();
				return null;
			}
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	public static void main(String[] args) {
		Handler handler = new Handler();
		handler.handleRequest(null, null);
	}
}
