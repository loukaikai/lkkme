package com.amarsoft.rwa.engine.rest;

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
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;
import java.util.concurrent.locks.Lock;

/**
 * @description: 任务调度接口
 * @author: chenqing
 * @create: 2022/4/10 11:06
 **/
@RestController
@CacheConfig
@RequestMapping("/task/")
@Slf4j
public class TaskRest {
    @Autowired
    private LockService lockService;
    @Autowired
    private TaskService taskService;
    /**
     * 市场风险调用接口
     * @param meTaskInfoDto
     * @return
     * @throws Exception
     */
    @RequestMapping("/rwame")
    @ResponseBody
    public ServiceResult rwaMeTask(@Validated @RequestBody MeTaskInfoDto meTaskInfoDto) throws Exception {
        Lock lock = lockService.getCallApiLock(meTaskInfoDto.getResultNo());
        lock.lock();
        // 数据库连接
        DBConnection db = new DBConnection();
        try {
            // 初始化引擎参数
            EngineUtil.initEngine();
            EngineUtil.initTaskJbo();
            EngineUtil.initConstantJbo();
            // 开始日志
            log.info(EngineService.getTopMsg(EngineJBO.version, meTaskInfoDto.getResultNo(),
                    meTaskInfoDto.getDataDate(), meTaskInfoDto.getTaskType(), meTaskInfoDto.getTaskID()));
            // 初始化
            TaskJBO.currentStep = null;
            TaskJBO.currentStepTime = null;
            TaskJBO.resultNo = meTaskInfoDto.getResultNo();
            TaskJBO.dataDate = EngineUtil.parseDate(meTaskInfoDto.getDataDate());
            TaskJBO.taskType = meTaskInfoDto.getTaskType();
            TaskJBO.schemeID = meTaskInfoDto.getTaskID();
            TaskJBO.startTime = new Date();
            TaskJBO.logID = meTaskInfoDto.getResultNo();

            // 获取数据库连接
            db.createConnection();
            //清理日志
            LogUtil.delTaskLog(db, TaskJBO.resultNo, EngineJBO.riskType);
            log.info("清理日志成功");
            // 插入任务日志
            LogUtil.insertTaskDetailLog(db);
            // 初始化步骤日志
            LogUtil.initStepLog(db);
            log.info("初始化日志成功");
            // 异步计算
            taskService.asyncExecuteRwaMeTask(meTaskInfoDto);
        } finally {
            // 关闭连接
            db.close();
            LockUtils.unlock(lock);
        }
        return ServiceResult.success();
    }
    /**
     * 市场风险执行状态查询
     * @param meTaskInfoDto
     * @return
     * @throws Exception
     */
    @RequestMapping("/query/status/rwame")
    @ResponseBody
    public ServiceResult queryStatusRwaMe(@Validated @RequestBody MeTaskInfoDto meTaskInfoDto) throws Exception {
        return ServiceResult.success(LogUtil.queryCalculationState(meTaskInfoDto.getResultNo(),EngineJBO.riskType));
    }
}
