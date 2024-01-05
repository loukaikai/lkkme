package com.amarsoft.rwa.engine.constant;

/**
 * @description: 锁类型
 * @author: chenqing
 * @create: 2023/1/12 17:15
 **/
public enum LockType implements ICodeEnum {

    EXE("EXE", "执行"),
    ST("ST", "单笔测算"),
    IMT("IMT", "即时任务"),
    JOB("JOB", "作业消费"),
    PROC("PROC", "存储过程"),
    CACHE("CACHE", "缓存"),
    ;

    private String code;
    private String name;

    private LockType(String code, String name) {
        this.code = code;
        this.name = name;
    }

    @Override
    public String getCode() {
        return code;
    }

    public String getName() {
        return name;
    }

}
