package com.amarsoft.rwa.engine.me.jbo;

import com.amarsoft.rwa.engine.me.step.StepType;

import java.util.Date;

/**
 * 计算任务计算相关信息类
 * <br>存放当前计算任务共享的计算相关信息。
 * 
 * @author 陈庆
 * @version 1.0 2013-06-06
 *
 */
public class TaskJBO {
	
	/** 结果流水号 */
	public static String resultNo;
	/** 任务类型 */
	public static String taskType;
	/** 任务名称 */
	//public static String taskName;
	/** 数据日期 */
	public static String dataDate;
	/** 数据流水号 */
	public static String dataNo;
	/** 任务日志ID */
	public static String logID;
	/** 任务开始时间 */
	public static Date startTime;
	/** 任务结束时间 */
	public static Date overTime;
	/** 是否并表 默认非并表 市场风险暂时只支持非并表*/
	public static String consolidatedFlag = "0";
	/** 计算方案ID */
	public static String schemeID;
	/** 计算方案名称 */
	public static String schemeName;
	/** 市场风险计算方法 */
	public static String marketApproach;
	/** 利率一般风险计算方法 */
	public static String mirrgrApproach;
	/** 市场风险参数版本流水号 */
	public static String marketParamVerNo;
	/** 信用风险参数版本流水号 */
	//public static String creditParamVerNo;
	/** 信用风险计算状态 */
	public static String creditState;
	/** 市场风险数据是否确认 */
	public static String confirmFlag;
	/** 当前计算步骤 */
	public static StepType currentStep;
	/** 当前计算步骤开始时间 */
	public static Date currentStepTime;
	/** 风险加权资产 */
	public static double RWA;
	/** 资本要求 */
	public static double RC;
	/** 一般风险资本要求 */
	public static double GRCR;
	/** 特定风险资本要求 */
	public static double SRCR;
	/** 资产证券化特定风险资本要求 */
	public static double ABSSRCR;
	/** 利率特定风险资本要求 */
	public static double IRRSRCR;
	/** 利率一般风险资本要求 */
	public static double IRRGRCR;
	/** 股票特定风险资本要求 */
	public static double ERSRCR;
	/** 股票一般风险资本要求 */
	public static double ERGRCR;
	/** 外汇一般风险资本要求 */
	public static double FERGRCR;
	/** 商品一般风险资本要求 */
	public static double CRGRCR;
	/** 期权一般风险资本要求 */
	public static double ORGRCR;
	/** 期权简易法存在基础工具对冲资本要求 */
	//public static double OSHSCR;
	/** 期权简易法只存在期权多头资本要求 */
	//public static double OSLPCR;
	/** 期权得尔塔+法利率期权资本要求 */
	public static double ODIROCR;
	/** 期权得尔塔+法股票期权资本要求 */
	public static double ODEOCR;
	/** 期权得尔塔+法外汇期权资本要求 */
	public static double ODFEOCR;
	/** 期权得尔塔+法商品期权资本要求 */
	public static double ODCOCR;

	/** 利率风险资本要求*/
	public static double IRR_CR;
	/** 外汇风险资本要求*/
	public static double FER_CR;
	/** 商品风险资本要求*/
	public static double CR_CR;
	/** 股票风险资本要求*/
	public static double ER_CR;

	
}
