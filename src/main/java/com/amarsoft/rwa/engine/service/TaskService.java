package com.amarsoft.rwa.engine.service;

import com.amarsoft.rwa.engine.entity.MeTaskInfoDto;
import com.amarsoft.rwa.engine.me.service.EngineService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

/**
 * @description:
 * @author: chenqing
 * @create: 2023/7/11 21:33
 **/
@Service
@Slf4j
public class TaskService {
    /**
     * 市场风险引擎调用
     * @param meTaskInfo
     */
    @Async("myTaskExecutor")
    public void asyncExecuteRwaMeTask(MeTaskInfoDto meTaskInfo) {
        try {
            EngineService.getInstance().execute(meTaskInfo.getResultNo(), meTaskInfo.getDataDate(), meTaskInfo.getTaskType(), meTaskInfo.getTaskID());
        } catch (Exception e){
            log.error("市场风险引擎执行失败，失败原因：", e);
        }
    }
}
