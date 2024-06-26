package com.task04;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.syndicate.deployment.annotations.events.SqsEvents;
import com.syndicate.deployment.annotations.events.SqsTriggerEventSource;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.model.RetentionSetting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@LambdaHandler(lambdaName = "sqs_handler",
		roleName = "sqs_handler-role",
		isPublishVersion = false,
		logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
@SqsTriggerEventSource(targetQueue = "async_queue", batchSize = 10)
public class SqsHandler implements RequestHandler<SQSEvent, Void>{

	private static final Logger logger = LogManager.getLogger(SqsHandler.class);

	@Override
	public Void handleRequest(SQSEvent event, Context context)
	{
		for(SQSEvent.SQSMessage msg : event.getRecords()){
			String message = "Received SQS message: " + msg.getBody();
			logger.info(message);
			System.out.println(message);
		}
		return null;
	}
}

