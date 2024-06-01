package com.task07;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.util.StringInputStream;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.syndicate.deployment.annotations.environment.EnvironmentVariable;
import com.syndicate.deployment.annotations.environment.EnvironmentVariables;
import com.syndicate.deployment.annotations.events.RuleEventSource;
import com.syndicate.deployment.annotations.events.RuleEvents;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.model.RetentionSetting;

import java.io.IOException;
import java.time.Instant;
import java.util.*;

@LambdaHandler(lambdaName = "uuid_generator",
	roleName = "uuid_generator-role",
	isPublishVersion = true,
	aliasName = "${lambdas_alias_name}",
	logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
@EnvironmentVariables({
				@EnvironmentVariable(key = "target_bucket", value = "${target_bucket}"),
				@EnvironmentVariable(key = "region", value = "${region}")
})
@RuleEvents(
		@RuleEventSource(targetRule = "uuid_trigger")
)
public class UuidGenerator implements RequestHandler<Object, String> {

	private AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();


	public String handleRequest(Object request, Context context) {
		LambdaLogger logger = context.getLogger();

		// Generate 10 random UUIDs
		List<String> ids = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			ids.add(UUID.randomUUID().toString());
		}
		Map<String, Object> content = new HashMap<>();
		content.put("ids", ids);
		ObjectMapper objectMapper = new ObjectMapper();
		String jsonString = "";
		try {
			jsonString = objectMapper.writeValueAsString(content);
		} catch (IOException e) {
			logger.log(e.getMessage());
		}

		// Prepare the file name
		String fileName = Instant.now().toString();

		// Write the content to a new file in the S3 bucket
		try {
			s3.putObject(new PutObjectRequest(
					System.getenv("target_bucket"),
					fileName,
					new StringInputStream(jsonString),
					new ObjectMetadata()));
		} catch (IOException e) {
			logger.log(e.getMessage());
		}


		return "File created successfully!";
	}
}
