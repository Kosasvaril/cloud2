package com.task05;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.model.RetentionSetting;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@LambdaHandler(lambdaName = "api_handler",
	roleName = "api_handler-role",
	isPublishVersion = false,
	logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
public class ApiHandler implements RequestHandler<Map<String, Object>, Map<String, Object>> {

private static final String DYNAMODB_TABLE_NAME = "Events";
private static final AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.defaultClient();
private static final ObjectMapper objectMapper = new ObjectMapper();

@Override
public Map<String, Object> handleRequest(Map<String, Object> input, Context context) {
	Map<String, Object> response = new HashMap<>();
	try {
		// Generate a UUID for the event ID
		String id = UUID.randomUUID().toString();

		// Get the current time in ISO 8601 format
		String createdAt = Instant.now().toString();

		// Get the principal ID and content from the input
		int principalId = (int) input.get("principalId");
		Map<String, String> content = (Map<String, String>) input.get("content");

		// Create a new event
		Map<String, AttributeValue> event = new HashMap<>();
		event.put("id", new AttributeValue(id));
		event.put("principalId", new AttributeValue().withN(String.valueOf(principalId)));
		event.put("createdAt", new AttributeValue(createdAt));
		event.put("body", new AttributeValue(objectMapper.writeValueAsString(content)));

		// Save the event to DynamoDB
		PutItemRequest putItemRequest = new PutItemRequest()
				.withTableName(DYNAMODB_TABLE_NAME)
				.withItem(event);
		PutItemResult putItemResult = dynamoDB.putItem(putItemRequest);

		// Create the response
		response.put("statusCode", 201);
		response.put("event", event);

	} catch (Exception e) {
		// Handle any errors
		response.put("statusCode", 500);
		response.put("error", e.getMessage());
	}
	return response;
}
}
