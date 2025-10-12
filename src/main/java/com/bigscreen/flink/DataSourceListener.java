package com.bigscreen.flink;


import com.bigscreen.config.flink.MySqlDateCustomConverterConfig;
import com.bigscreen.config.flink.MysqlDeserializationConfig;
import com.bigscreen.entity.BinlogChangeInfo;
import com.bigscreen.entity.CDCSourceProperties;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import jakarta.annotation.Resource;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * @author Xu huaiang
 * @date 2024/08/03
 * @description 实现ApplicationRunner接口，实现在应用启动时执行初始化操作，监听mysql数据库的变更
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DataSourceListener implements ApplicationRunner {

    @Resource
    private DataChangeSink dataChangeSink;

    @Resource
    private CDCSourceProperties cdcSourceProperties;

    @Resource
    private MySqlDateCustomConverterConfig mySqlDateCustomConverterConfig;

    // 用于管理多个Flink作业
    private final Map<String, CompletableFuture<Void>> runningJobs = new ConcurrentHashMap<>();

    @Override
    public void run(ApplicationArguments args) {
        log.info("开始启动多数据源CDC监听...");
        // 为每个数据源启动独立的监听任务
        startAllDataSourceListeners();
        // 注册优雅关闭钩子
        registerShutdownHook();
    }

    /**
     * 启动所有数据源的监听
     */
    private void startAllDataSourceListeners() {
        // 获取所有数据源配置
        Map<String, CDCSourceProperties.SourceConfig> sourceConfigs = cdcSourceProperties.getSources();

        if (sourceConfigs == null || sourceConfigs.isEmpty()) {
            log.warn("未配置任何数据源，请检查配置");
            return;
        }

        log.info("发现 {} 个数据源需要监听", sourceConfigs.size());
        // 遍历 Map 的 entrySet
        sourceConfigs.forEach((sourceName, config) -> {
            // 使用配置中的名称作为 sourceKey
            String sourceKey = buildSourceKey(config, sourceName);
            CompletableFuture<Void> jobFuture = CompletableFuture.runAsync(() -> {
                startSingleDataSourceListener(config, sourceKey);
            });

            runningJobs.put(sourceKey, jobFuture);
            log.info("数据源 {} 监听任务已启动", sourceKey);
        });

    }

    /**
     * 构建数据源唯一标识
     */
    private String buildSourceKey(CDCSourceProperties.SourceConfig config, String sourceName) {
        // 使用配置名称 + IP + 数据库名构建唯一key
        return String.format("%s-%s-%s",
                sourceName,
                config.getIp(),
                String.join(",", config.getDatabases())
        );
    }

    /**
     * 启动单个数据源的监听
     */
    private void startSingleDataSourceListener(CDCSourceProperties.SourceConfig config,
                                               String sourceKey) {
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            // 配置Flink环境
            configureFlinkEnvironment(env, config);
            // 构建数据源
            MySqlSource<BinlogChangeInfo> mySqlSource = buildDataChangeSource(config);
            // 创建数据流
            DataStream<BinlogChangeInfo> streamSource = env
                    .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), sourceKey + "-source")
                    .setParallelism(1); // 保持消息顺序
            // 发送到Sink
            streamSource.addSink(dataChangeSink);
            log.info("开始执行数据源 {} 的Flink作业", sourceKey);
            env.executeAsync(sourceKey + "-cdc-job");
        } catch (Exception e) {
            log.error("数据源 {} 的Flink作业执行失败", sourceKey, e);
            throw new RuntimeException(e);
        }
    }

    /**
     * 配置Flink环境
     */
    private void configureFlinkEnvironment(StreamExecutionEnvironment env,
                                           CDCSourceProperties.SourceConfig config) {
        // 设置并行度，可根据数据源配置不同的并行度
        Integer parallelism = config.getParallelism() != null ?
                config.getParallelism() : 2;
        env.setParallelism(parallelism);
        // 开启Checkpoint
        env.enableCheckpointing(30000);
        // 配置重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // 尝试重启次数
                Time.of(10, TimeUnit.SECONDS) // 延迟时间
        ));

        // 生产环境建议配置状态后端
        // env.setStateBackend(new HashMapStateBackend());
        // env.getCheckpointConfig().setCheckpointStorage("hdfs:///checkpoints");
    }

    /**
     * 构造单个数据源的变更数据源
     */
    private MySqlSource<BinlogChangeInfo> buildDataChangeSource(CDCSourceProperties.SourceConfig config) {
        return MySqlSource.<BinlogChangeInfo>builder()
                .hostname(config.getIp())
                .port(config.getPort())
                .username(config.getUsername())
                .password(config.getPassword())
                .databaseList(config.getDatabases())
                .tableList(config.getTables())
                .startupOptions(getStartupOptions(config)) // 设置启动模式，默认为initial:初始化快照,即全量导入后增量导入(检测更新数据写入)
                .serverId(String.valueOf(generateServerId(config))) // 为每个数据源生成唯一serverId
                .deserializer(new MysqlDeserializationConfig())
                .debeziumProperties(getDebeziumProperties(config))
                .serverTimeZone(config.getServerTimeZone() != null ?
                        config.getServerTimeZone() : "GMT+8")
                .build();
    }

    /**
     * 生成唯一serverId
     */
    private long generateServerId(CDCSourceProperties.SourceConfig config) {
        // 基于IP和端口生成唯一ID
        String uniqueKey = config.getIp() + ":" + config.getPort();
        return Math.abs(uniqueKey.hashCode()) % 100000 + 5400;
    }

    /**
     * 获取启动选项，默认全量+增量
     */
    private StartupOptions getStartupOptions(CDCSourceProperties.SourceConfig config) {
        if (config.getStartupMode() != null) {
            switch (config.getStartupMode().toLowerCase()) {
                case "initial":
                    return StartupOptions.initial(); // 初始模式（全量+增量）
                case "latest":
                    return StartupOptions.latest(); // 仅增量
                case "timestamp":
                    if (config.getStartupTimestamp() != null) {
                        return StartupOptions.timestamp(config.getStartupTimestamp()); // 从指定的时间戳开始读取
                    }
                    break;
            }
        }
        return StartupOptions.initial();
    }

    /**
     * Debezium参数配置
     */
    private Properties getDebeziumProperties(CDCSourceProperties.SourceConfig config) {
        Properties debeziumProperties = new Properties();

        // 基础配置
        debeziumProperties.setProperty("converters", "dateConverters");
        debeziumProperties.setProperty("dateConverters.type", mySqlDateCustomConverterConfig.getClass().getName());

        // 可针对不同数据源配置不同的Debezium参数
        if (config.getDebeziumProperties() != null) {
            debeziumProperties.putAll(config.getDebeziumProperties());
        }

        // 心跳配置，防止没有数据更新时连接断开
        debeziumProperties.setProperty("heartbeat.interval.ms", "30000");

        return debeziumProperties;
    }

    /**
     * 注册优雅关闭钩子
     */
    private void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("收到关闭信号，正在停止所有数据源监听...");
            stopAllDataSourceListeners();
        }));
    }

    /**
     * 停止所有数据源监听
     */
    public void stopAllDataSourceListeners() {
        log.info("正在停止 {} 个数据源监听任务", runningJobs.size());

        runningJobs.forEach((sourceKey, future) -> {
            try {
                if (future != null && !future.isDone()) {
                    future.cancel(true);
                    log.info("数据源 {} 监听任务已停止", sourceKey);
                }
            } catch (Exception e) {
                log.error("停止数据源 {} 监听任务失败", sourceKey, e);
            }
        });

        runningJobs.clear();
    }

    /**
     * 重启指定数据源的监听
     */
    public boolean restartDataSourceListener(String sourceKey) {
        // 实现重启逻辑
        log.info("重启数据源 {} 的监听", sourceKey);
        return true;
    }

    /**
     * 获取运行状态
     */
    public Map<String, String> getDataSourceStatus() {
        Map<String, String> status = new HashMap<>();
        runningJobs.forEach((sourceKey, future) -> {
            if (future.isDone()) {
                status.put(sourceKey, "STOPPED");
            } else {
                status.put(sourceKey, "RUNNING");
            }
        });
        return status;
    }
}

