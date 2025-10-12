package com.bigscreen.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class InstanceCompareInfo {
    private Long id;

    private Map<String, Object> instanceCompareMap;
}
