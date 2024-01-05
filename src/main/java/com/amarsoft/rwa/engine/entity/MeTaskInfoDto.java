package com.amarsoft.rwa.engine.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

/**
 * @description: 任务信息
 * @author: chenqing
 * @create: 2022/4/10 11:19
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
public class MeTaskInfoDto {

    @Pattern(regexp = "[0-9A-Za-z]{10,}", message = "结果号输入异常")
    @NotNull(message = "结果号不能为空")
    private String resultNo;
    @Pattern(regexp = "[0-9A-Za-z]{1,}", message = "数据批次号输入异常")
    @NotNull(message = "数据批次号不能为空")
    private String dataDate;
    @NotNull(message = "计算类型不能为空")
    private String taskType;
    @NotNull(message = "任务ID不能为空")
    private String taskID;

}
