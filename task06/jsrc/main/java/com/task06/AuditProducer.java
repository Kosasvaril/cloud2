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
import com.syndicate.deployment.model.RetentionSetting;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@LambdaHandler(lambdaName = "audit_producer",
	roleName = "audit_producer-role",
	isPublishVersion = true,
	aliasName = "${lambdas_alias_name}",
	logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
@EnvironmentVariables(@EnvironmentVariable(key="name", value="${target_table}"))
@DynamoDbTriggerEventSource(targetTable = "Configuration", batchSize = 100)
public class AuditProducer implements RequestHandler<DynamodbEvent, Map<String, Object>> {

	private AmazonDynamoDB amazonDynamoDB;
	private DynamoDB dynamoDB;
	private Table table;

	private static final String DYNAMODB_TABLE_NAME = System.getenv("name");
	private final Regions REGION = Regions.EU_CENTRAL_1;

	public Map<String, Object> handleRequest(DynamodbEvent event, Context context) {
		initDynamoDbClient();
		/*for (DynamodbEvent.DynamodbStreamRecord record : event.getRecords()) {
			if ("MODIFY".equals(record.getEventName())) {
				Map<String, AttributeValue> oldImage = record.getDynamodb().getOldImage();
				Map<String, AttributeValue> newImage = record.getDynamodb().getNewImage();

				if (!oldImage.get("value").equals(newImage.get("value"))) {
					// Create a new audit entry
					Item item = new Item()
							.withPrimaryKey("id", new AttributeValue(UUID.randomUUID().toString()))
							.with("itemKey", newImage.get("key"))
							.withString("modificationTime", Instant.now().toString())
							.withString("updatedAttribute","value")
							.withNumber("oldValue", Integer.valueOf((oldImage.get("value").toString())))
							.withNumber("newValue", Integer.valueOf((newImage.get("value").toString())));

					this.table.putItem(item);
				}
			}
		}*/
		return null;
	}

	private void initDynamoDbClient() {
		this.amazonDynamoDB = AmazonDynamoDBClientBuilder.standard()
				.withRegion(REGION)
				.build();
		this.dynamoDB = new DynamoDB(amazonDynamoDB);
		this.table = dynamoDB.getTable(DYNAMODB_TABLE_NAME);
	}
}
