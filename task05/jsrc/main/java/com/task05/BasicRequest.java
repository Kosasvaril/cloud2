package com.task05;

import lombok.Data;

import java.util.Map;

@Data
public class BasicRequest {
    private int principalId;
    private Map<String, Object> content;
    // standard getters and setters
}