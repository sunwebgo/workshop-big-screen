package com.bigscreen.entity;

import lombok.Data;

@Data
public class BinlogChangeInfo {
    /**
     * 数据库名
     */
    private String database;
    /**
     * 表名
     */
    private String tableName;
    /**
     * 变更类型
     */
    private String operatorType;
    /**
     * 变更前数据
     */
    private String beforeData;
    /**
     * 变更后数据
     */
    private String afterData;
    /**
     * 操作的数据
     */
    private String data;
    /**
     * binlog文件名
     */
    private String fileName;
    /**
     * binlog当前读取点位
     */
    private Integer filePos;
    /**
     * 变更时间
     */
    private Long operatorTime;

}
