package com.task05;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
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
public class ApiHandler implements RequestHandler<BasicRequest, Map<String, Object>> {

	private AmazonDynamoDB amazonDynamoDB;
	private DynamoDB dynamoDB;
	private Table table;

	private String DYNAMODB_TABLE_NAME = "Events";
	private Regions REGION = Regions.EU_CENTRAL_1;

	@Override
	public Map<String, Object> handleRequest(BasicRequest input, Context context) {
		this.initDynamoDbClient();
		return persistData(input);
	}

	private Map<String, Object> persistData(BasicRequest basicRequest) throws ConditionalCheckFailedException {
		String id = UUID.randomUUID().toString();
		String createdAt = Instant.now().toString();
		Item item = new Item()
				.withPrimaryKey("id", id)
				.withNumber("principalId", basicRequest.getPrincipalId())
				.withString("createdAt", createdAt)
				.withMap("body", basicRequest.getContent());

		this.table.putItem(item);

		return Map.of(
				"statusCode", 201,
				"event", Map.of(
						"id", id,
						"principalId", basicRequest.getPrincipalId(),
						"createdAt", createdAt,
						"body", basicRequest.getContent()
				)
		);
	}

	private void initDynamoDbClient() {
		this.amazonDynamoDB = AmazonDynamoDBClientBuilder.standard()
				.withRegion(REGION)
				.build();
		this.dynamoDB = new DynamoDB(amazonDynamoDB);
		this.table = dynamoDB.getTable(DYNAMODB_TABLE_NAME);
	}
}
