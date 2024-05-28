package com.task04;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.syndicate.deployment.annotations.events.SnsEventSource;
import com.syndicate.deployment.annotations.events.SnsEvents;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.model.RetentionSetting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@LambdaHandler(lambdaName = "sns_handler",
	roleName = "sns_handler-role",
	isPublishVersion = false,
	logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
@SnsEvents(@SnsEventSource(targetTopic = "lambda_topic"))
public class SnsHandler implements RequestHandler<SNSEvent, String> {

	// Configure the logger
	private static final Logger logger = LogManager.getLogger(SnsHandler.class);

	@Override
	public String handleRequest(SNSEvent event, Context context) {
		// Iterate over each record in the SNS event
		for (SNSEvent.SNSRecord record : event.getRecords()) {
			// Get the SNS message body
			String message = record.getSNS().getMessage();

			// Log the message body to CloudWatch
			logger.info("Received SNS message: {}", message);
		}
		return "Successfully processed SNS messages";
	}
}
