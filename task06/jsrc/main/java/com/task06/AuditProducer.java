package com.task06;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.syndicate.deployment.annotations.environment.EnvironmentVariable;
import com.syndicate.deployment.annotations.environment.EnvironmentVariables;
import com.syndicate.deployment.annotations.events.DynamoDbTriggerEventSource;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.model.RetentionSetting;

import java.time.Instant;
import java.util.Collections;
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
public class AuditProducer implements RequestHandler<DynamodbEvent, Map<String, Object>> {
    private Table tableAudit;
	private static final Regions REGION = Regions.EU_CENTRAL_1;
	private static final String DYNAMODB_TABLE_NAME_AUDIT = System.getenv("name");
	Gson gson = new GsonBuilder().setPrettyPrinting().create();


	public Map<String, Object> handleRequest(DynamodbEvent event, Context context) {
		initDynamoDbClientAudit();
		LambdaLogger logger = context.getLogger();
		logger.log("Record from event: "+gson.toJson(event));
		for (DynamodbEvent.DynamodbStreamRecord r : event.getRecords()) {
			if ("INSERT".equals(r.getEventName())) {

				Map<String, AttributeValue> newImage = r.getDynamodb().getNewImage();
				Map<String, Object> auditCreationMap = new HashMap<>();
				auditCreationMap.put("key", newImage.get("key").getS());
				auditCreationMap.put("value", Integer.parseInt(newImage.get("value").getN()));
				Item auditInsertItem = new Item()
						.withPrimaryKey("id", UUID.randomUUID().toString())
						.withString("itemKey", newImage.get("key").getS())
						.withString("modificationTime", Instant.now().toString())
						.withMap("newValue", auditCreationMap);

				this.tableAudit.putItem(auditInsertItem);

			}else if("MODIFY".equals(r.getEventName())){
				Map<String, AttributeValue> newImage = r.getDynamodb().getNewImage();
				Map<String, AttributeValue> oldImage = r.getDynamodb().getOldImage();

				// Create a new entry for the Audit table
				Item auditModifyItem = new Item()
						.withPrimaryKey("id", UUID.randomUUID().toString())
						.withString("itemKey", newImage.get("key").getS())
						.withString("modificationTime", Instant.now().toString())
						.withString("updatedAttribute", "value")
						.withNumber("oldValue", Integer.parseInt(oldImage.get("value").getN()))
						.withNumber("newValue", Integer.parseInt(newImage.get("value").getN()));

				// Put the item into the table
				this.tableAudit.putItem(auditModifyItem);
			}
		}
        return Collections.emptyMap();
    }

	private void initDynamoDbClientAudit() {
        AmazonDynamoDB amazonDynamoDBAudit = AmazonDynamoDBClientBuilder.standard()
                .withRegion(REGION)
                .build();
        DynamoDB dynamoDBAudit = new DynamoDB(amazonDynamoDBAudit);
		this.tableAudit = dynamoDBAudit.getTable(DYNAMODB_TABLE_NAME_AUDIT);
	}
}
