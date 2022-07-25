package aws_lambda;

import java.util.Map;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

public class Handler implements RequestHandler<Map<String, String>, String> {
	// You can add initialization code outside of your handler method to reuse
	// resources across multiple invocations. When the runtime loads your handler,
	// it runs static code and the class constructor. Resources that are created
	// during initialization stay in memory between invocations, and can be reused
	// by the handler thousands of times.

	// init DynamoDB here and reuse
	// download config file from S3 in handleRequest

	@Override
	public String handleRequest(Map<String, String> input, Context context) {
		System.out.println("Input " + input);
		System.out.println("Context " + context);
		return null;
	}

}
