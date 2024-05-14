package com.task02;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.annotations.lambda.LambdaUrlConfig;
import com.syndicate.deployment.model.RetentionSetting;
import com.syndicate.deployment.model.lambda.url.AuthType;
import com.syndicate.deployment.model.lambda.url.InvokeMode;

import java.util.HashMap;
import java.util.Map;

@LambdaHandler(lambdaName = "hello_world",
	roleName = "hello_world-role",
	logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
@LambdaUrlConfig(authType = AuthType.NONE, invokeMode = InvokeMode.BUFFERED )
public class HelloWorld implements RequestHandler<Object, Map<String, Object>> {
	public Map<String, Object> handleRequest(Object request, Context context) {
		Map<String, Object> event = (Map<String, Object>) request;
		String rawPath = (String) event.get("rawPath");
		Map<String, Object> resultMap = new HashMap<>();

		if(rawPath.equals("/hello")){
			resultMap.put("statusCode", 200);
			resultMap.put("body", "{\"message\": \"Hello from Lambda\" }");
			return resultMap;
		}
		Map<String, Object> requestContext = (Map<String, Object>) event.get("requestContext");
		Map<String, Object> http = (Map<String, Object>) requestContext.get("http");
		resultMap.put("statusCode", 400);
		resultMap.put("body",
				"{ \"message\": \"Bad request syntax or unsupported method. Request path: {"+rawPath+"}. HTTP method: {"+http.get("method")+"}\" }");
		return resultMap;
	}
}
