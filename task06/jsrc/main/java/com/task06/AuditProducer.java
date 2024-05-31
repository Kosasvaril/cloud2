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
@EnvironmentVariables({
		@EnvironmentVariable(key="name2", value="${target_table2}"),
		@EnvironmentVariable(key="name", value="${target_table}")}
)
//@DependsOn(name = "Configuration", resourceType = ResourceType.DYNAMODB_TABLE)
public class AuditProducer implements RequestHandler<DynamodbEvent, Map<String, Object>> {

	private AmazonDynamoDB amazonDynamoDB;
	private DynamoDB dynamoDB;
	private Table table;
	private final Regions REGION = Regions.EU_CENTRAL_1;
	private String DYNAMODB_TABLE_NAME = System.getenv("name2");
	private String DYNAMODB_TABLE_NAME2 = System.getenv("name");


	public Map<String, Object> handleRequest(DynamodbEvent event, Context context) {

		for (DynamodbEvent.DynamodbStreamRecord record : event.getRecords()) {
			if ("INSERT".equals(record.getEventName())) {
				Map<String, AttributeValue> newImage = record.getDynamodb().getNewImage();

				Item confItem = new Item()
						.withPrimaryKey("key", newImage.get("key").getS())
						.withNumber("value", Integer.parseInt(newImage.get("value").getN()));

				// Save the entry to the first table
				initDynamoDbClient(DYNAMODB_TABLE_NAME);
				this.table.putItem(confItem);

				Map<String, Object> auditCreationMap = new HashMap<>();
				auditCreationMap.put("key", newImage.get("key").getS());
				auditCreationMap.put("value", Integer.parseInt(newImage.get("value").getN()))

				Item auditItem = new Item()
						.withPrimaryKey("id", UUID.randomUUID().toString())
						.withString("itemKey", newImage.get("key").getS())
						.withString("modificationTime", Instant.now().toString())
						.withMap("newValue", auditCreationMap);
				initDynamoDbClient(DYNAMODB_TABLE_NAME2);
				this.table.putItem(auditItem);
			}


			System.out.println(record);
		}
		Map<String, Object> resultMap = new HashMap<String, Object>();
		resultMap.put("statusCode", 200);
		resultMap.put("body", "Hello from Lambda");
		return resultMap;
	}

	private void initDynamoDbClient(String tableName) {
		this.amazonDynamoDB = AmazonDynamoDBClientBuilder.standard()
				.withRegion(REGION)
				.build();
		this.dynamoDB = new DynamoDB(amazonDynamoDB);
		this.table = dynamoDB.getTable(tableName);
	}
}
