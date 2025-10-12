package com.bigscreen.entity;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.*;

@Data
@Configuration
@ConfigurationProperties(prefix = "cdc")
public class CDCSourceProperties {
    private Map<String, SourceConfig> sources = new HashMap<>();

    @Data
    public static class SourceConfig {
        private String ip;

        private Integer port = 3306;

        private String username;

        private String password;

        private String[] databases;

        private String[] tables;

        private Integer parallelism = 2;

        private String startupMode = "initial";

        private Long startupTimestamp;

        private String serverTimeZone = "GMT+8";

        private Map<String, String> debeziumProperties;

        private Boolean enabled = true;

        private String description;
    }
}
