package com.amarsoft.rwa.engine.batch;

import com.alibaba.fastjson.JSON;
import com.amarsoft.rwa.engine.config.ServiceResult;
import com.amarsoft.rwa.engine.entity.MeTaskInfoDto;
import com.amarsoft.rwa.engine.me.jbo.EngineJBO;
import com.amarsoft.rwa.engine.me.jbo.TaskJBO;
import com.amarsoft.rwa.engine.me.service.EngineService;
import com.amarsoft.rwa.engine.me.util.EngineUtil;
import com.amarsoft.rwa.engine.me.util.db.DBConnection;
import com.amarsoft.rwa.engine.me.util.log.LogUtil;
import com.amarsoft.rwa.engine.service.LockService;
import com.amarsoft.rwa.engine.service.TaskService;
import com.amarsoft.rwa.engine.util.LockUtils;
import com.xxl.job.core.handler.annotation.XxlJob;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import lombok.extern.slf4j.Slf4j;

/**
 * @author 16509
 * @version 1.0
 * @project rwame_20231217
 * @description
 * @date 12/26/2023 17:40:22
 */
@Slf4j
public class RwaStatusTaskJobHandler {

    @Autowired
    private LockService lockService;

    @Autowired
    private TaskService taskService;

    @XxlJob("rwaStatusTaskJobHandler")
    public ServiceResult rwaStatusTaskJobHandler(String param) throws Exception {
        log.info("========>定时任务：市场风险引擎调用状态查询");
        MeTaskInfoDto meTaskInfoDto = JSON.parseObject(param, MeTaskInfoDto.class);
        return ServiceResult.success(LogUtil.queryCalculationState(meTaskInfoDto.getResultNo(),EngineJBO.riskType));
    }
}
