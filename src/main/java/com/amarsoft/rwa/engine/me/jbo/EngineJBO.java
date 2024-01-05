package com.amarsoft.rwa.engine.me.jbo;

import java.util.List;

/**
 * 引擎计算时的公共参数类
 * <br>
 * @author 陈庆
 * @version 2013-06-05
 *
 */
public class EngineJBO {
	/** 数据库类型 */
	public static String dbType;
	
	/** 计算引擎版本 */
	public static String version = "1.0";
	
	/** 计算引擎风险类型 */
	public static String riskType = "02";
	
	/** 本行的组织机构代码 */
	public static String orgCode;
	
	/** 子公司的机构ID */
	public static List<String> subOrgList;
	
	/** 引擎计算时的通用批量处理数 */
	public static int batchCount;
	
	/** 数据校验不通过是否继续运行,0继续,其他结束 */
	public static int checkRunFlag;
	
	/** 数据校验不通过程序返回结果,0或者1,0为成功,1为失败*/
	public static int checkFalseResult;
	/** 是否从客户表取客户信息，如果是0代表从市场风险暴露表取数，1代表从客户表取数*/
	public static int enableClientInfo;

}
