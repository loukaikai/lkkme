package com.amarsoft.rwa.engine.me.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 * @author qzf
 * 引擎参数初始化
 */
@Configuration
public class EngineConfig {
    //引擎计算时的通用批量处理数
    @Value("${engine.batchcount:10000}")
    private int batchCount;
    //引擎计算时的通用批量处理数
    @Value("${engine.checkfalseresult:0}")
    private int checkFalseResult;
    //引擎计算时的通用批量处理数
    @Value("${engine.checkrunflag:1}")
    private int checkRunFlag;
    //引擎计算时的通用批量处理数
    @Value("${engine.threadcount:10}")
    private int threadCount;
    //引擎计算时的通用批量处理数
    @Value("${engine.orgcode:XN9999}")
    private String orgCode;
    //引擎计算时的通用批量处理数
    @Value("${engine.subcompanyorgid:XN999999}")
    private String subCompanyOrgID;
    @Value("${engine.enableclientinfo:0}")
    private int enableClientInfo;

    public int getBatchCount() {
        return batchCount;
    }

    public int getCheckFalseResult() {
        return checkFalseResult;
    }

    public int getCheckRunFlag() {
        return checkRunFlag;
    }

    public int getThreadCount() {
        return threadCount;
    }

    public String getOrgCode() {
        return orgCode;
    }

    public String getSubCompanyOrgID() {
        return subCompanyOrgID;
    }

    public int getEnableClientInfo() {
        return enableClientInfo;
    }
}
