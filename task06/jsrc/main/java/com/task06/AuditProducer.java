package com.task06;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;
import com.syndicate.deployment.annotations.environment.EnvironmentVariable;
import com.syndicate.deployment.annotations.environment.EnvironmentVariables;
import com.syndicate.deployment.annotations.events.DynamoDbTriggerEventSource;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.annotations.resources.DependsOn;
import com.syndicate.deployment.model.ResourceType;
import com.syndicate.deployment.model.RetentionSetting;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@LambdaHandler(lambdaName = "audit_producer",
	roleName = "audit_producer-role",
	isPublishVersion = false,
	logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
@DynamoDbTriggerEventSource(targetTable = "Configuration", batchSize = 10)
@EnvironmentVariables(@EnvironmentVariable(key="name", value="${target_table}"))
//@DependsOn(name = "Configuration", resourceType = ResourceType.DYNAMODB_TABLE)
public class AuditProducer implements RequestHandler<DynamodbEvent, Map<String, Object>> {

	private AmazonDynamoDB amazonDynamoDB;
	private DynamoDB dynamoDB;
	private Table table;
	private final Regions REGION = Regions.EU_CENTRAL_1;
	private String DYNAMODB_TABLE_NAME = System.getenv("name");


	public Map<String, Object> handleRequest(DynamodbEvent event, Context context) {
		initDynamoDbClient();
		for (DynamodbEvent.DynamodbStreamRecord record : event.getRecords()) {
			System.out.println(record);
		}
		Map<String, Object> resultMap = new HashMap<String, Object>();
		resultMap.put("statusCode", 200);
		resultMap.put("body", "Hello from Lambda");
		return resultMap;
	}

	private void initDynamoDbClient() {
		this.amazonDynamoDB = AmazonDynamoDBClientBuilder.standard()
				.withRegion(REGION)
				.build();
		this.dynamoDB = new DynamoDB(amazonDynamoDB);
		this.table = dynamoDB.getTable(DYNAMODB_TABLE_NAME);
	}
}
