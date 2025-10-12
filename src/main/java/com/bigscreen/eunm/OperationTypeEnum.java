package com.bigscreen.eunm;

/**
 * @author Xu huaiang
 * @date 2024/08/03
 * @description 操作类型枚举 READ CREATE UPDATE DELETE
 */
public enum OperationTypeEnum {
    READ("READ"), // 全量同步

    CREATE("CREATE"), // 增量同步

    DELETE("DELETE"), // 删除

    UPDATE("UPDATE"); // 更新

    private String type;

    OperationTypeEnum(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public static OperationTypeEnum getByType(String type) {
        for (OperationTypeEnum operationTypeEnum : OperationTypeEnum.values()) {
            if (operationTypeEnum.getType().equals(type)) {
                return operationTypeEnum;
            }
        }
        return null;
    }


}
