package com.amarsoft.rwa.engine.me.service;

import cn.hutool.core.util.StrUtil;
import com.amarsoft.rwa.engine.me.exception.EngineParameterException;
import com.amarsoft.rwa.engine.me.exception.EngineSQLException;
import com.amarsoft.rwa.engine.me.jbo.EngineJBO;
import com.amarsoft.rwa.engine.me.jbo.TaskJBO;
import com.amarsoft.rwa.engine.me.step.StepFactory;
import com.amarsoft.rwa.engine.me.step.StepType;
import com.amarsoft.rwa.engine.me.util.DBUtils;
import com.amarsoft.rwa.engine.me.util.db.DBConnection;
import com.amarsoft.rwa.engine.me.util.log.LogUtil;
import lombok.extern.slf4j.Slf4j;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

/**
 * 计算引擎计算服务类
 * <br>计算引擎的服务入口，传入任务ID与数据日期，进行计算
 * 
 * @author 陈庆
 * @version 1.0 2015-08-18
 *
 */
@Slf4j
public class EngineService {
	/** 单例引擎服务 */
	private static final EngineService es = new EngineService();
	/** 任务状态标识 */
	private boolean taskOverFlag = false;
	/** 任务日志初始化标识 */
	private boolean taskLogFlag = false;
	
	/**
	 * 私有
	 */
	private EngineService() {}
	
	/**
	 * 获取单例实例
	 * 
	 * @return 返回单例实例
	 */
	public static EngineService getInstance() {
		return es;
	}
	
	/**
	 * 执行计算
	 * @param resultNo 结果流水号
	 * @param dataDate 数据日期
	 * @param taskType 任务类型
	 * @param schemeId 计算方案ID
	 */
	public void execute(String resultNo, String dataDate, String taskType, String schemeId) {
/*		// 开始日志
		log.info(this.getTopMsg(EngineJBO.version, resultNo, dataDate, taskType, schemeId));
		// 初始化
		TaskJBO.currentStep = null;
		TaskJBO.currentStepTime = null;
		TaskJBO.resultNo = resultNo;
		TaskJBO.dataDate = EngineUtil.parseDate(dataDate);
		TaskJBO.taskType = taskType;
		TaskJBO.schemeID = schemeId;
		TaskJBO.startTime = new Date();
		TaskJBO.logID = resultNo;*/
		// 数据库连接
		DBConnection db = new DBConnection();
		
		try {
			// 获取数据库连接
			db.createConnection();
			taskLogFlag = true;
			/*//清理日志
			LogUtil.delTaskLog(db, TaskJBO.resultNo, EngineJBO.riskType);
			log.info("清理日志成功");
			// 插入任务日志
			LogUtil.insertTaskDetailLog(db);
			taskLogFlag = true;

			// 初始化步骤日志
			LogUtil.initStepLog(db);
			log.info("初始化日志成功");*/

			// 清空计算临时表及结果临时表的数据
			this.clearTempTableData(db);

			
			// 初始化任务信息
			this.initTaskInfo(db, taskType, schemeId);
			
			// 即时任务，若已设置市场风险相关参数，才需继续计算 TODO 暂不支持
/*			if ("02".equals(TaskJBO.taskType)
				&& (TaskJBO.schemeID == null || "".equals(TaskJBO.schemeID) 
					|| TaskJBO.marketParamVerNo == null || "".equals(TaskJBO.marketParamVerNo))) {
				log.info("该即时任务市场风险不计算...");
				// 任务结束
				taskOverFlag = true;
				TaskJBO.overTime = new Date();
				// 更新任务正常结束日志，完成
				LogUtil.updateTaskDetailLog(db, "03");
				return;
			}*/
			
			// 初始化任务相关信息
			this.initTaskOtherInfo(db);
			
			// 任务相关参数校验
			this.checkTaskParams();
			log.info("任务[初始化]结束...");
			
			/* 执行各个步骤 */
			// 数据加载
			StepFactory.getStep(StepType.LOADDATA).execute();
			// 参数映射
			StepFactory.getStep(StepType.MAPPINGPARAMS).execute();
			// RWA计算
			StepFactory.getStep(StepType.CALCULATERWA).execute();
			// 结果插入
			StepFactory.getStep(StepType.INSERTRESULT).execute();
			
			// 任务结束
			TaskJBO.currentStep = null;
			TaskJBO.currentStepTime = null;
			taskOverFlag = true;
			TaskJBO.overTime = new Date();
			// 更新任务正常结束日志，完成
			LogUtil.updateTaskDetailLog(db, "03");
		} catch (Exception e) {
			this.logProcess(db, e);
		} finally {
			// 关闭连接
			db.close();
			//统计分析
			DBUtils.analyzeTable("RWA_EL_TaskDetail");
			DBUtils.analyzeTable("RWA_EL_Step");
			// 结束日志
			log.info(this.getBottomMsg());
		}
	}
	
	/**
	 * 异常的日志等相关处理
	 * @param db 数据库连接
	 * @param t 异常类
	 */
	private void logProcess(DBConnection db, Throwable t) {
		TaskJBO.overTime = new Date();
		try {
			if (TaskJBO.currentStep == null) {
				// 若任务未到结束，则记录初始化日志，否则记录结束日志
				if (!taskOverFlag) {
					log.error("任务[初始化]出现" + t, t);
				} else {
					log.error("任务[结束]出现" + t, t);
				}
				// 若已初始化任务日志，更新日志，失败
				if (taskLogFlag) {
					LogUtil.updateTaskDetailLog(db, "04");
				}
			} else {
				log.error(TaskJBO.currentStep + "出现" + t, t);
				LogUtil.updateTaskDetailLog(db, "04");
				LogUtil.updateStepLogByException(db, t);
			}
		} catch (EngineSQLException e) {
			// 以上操作若出异常直接打印日志
			log.error("异常日志相关处理出现数据库操作异常", e);
		}
	}
	
	/**
	 * 获取引擎的开始注释信息
	 * @param version 计算引擎版本
	 * @param sDate 输入日期
	 * @param taskType 任务类型
	 * @param schemeId 计算方案ID
	 * @return 返回引擎的开始注释信息
	 */
	public static String getTopMsg(String version, String resultNo, String sDate, String taskType, String schemeId) {
		String bs = "################################################################################";
		int bn = bs.length();
		String msg = "";
		if ("01".equals(taskType)) {
			// 常规任务
			msg = "\n" + bs + "\n" 
					+ getMsg("# RWA市场风险计算引擎" + version, bn)
					+ getMsg("# 数据期次：" + sDate, bn)
					+ getMsg("# 任务类型：常规任务[" + schemeId + "][" + resultNo + "]", bn)
					+ bs + "\n";
		} else {
			// 即时任务
			msg = "\n" + bs + "\n" 
					+ getMsg("# RWA市场风险计算引擎" + version, bn)
					+ getMsg("# 业务日期：" + sDate, bn)
					+ getMsg("# 任务类型：即时任务[" + schemeId + "][" + resultNo + "]", bn)
					+ bs + "\n";
		}
		return msg;
	}
	
	/**
	 * 获取格式化后的打印信息
	 * @param s 打印信息
	 * @param length 格式长度
	 * @return 返回格式化后的打印信息
	 */
	private static String getMsg(String s, int length) {
		int m = s.getBytes().length;
		StringBuffer sb = new StringBuffer(s);
		if (m < length - 1) {
			for (int i = 0; i < (length - m - 1); i++) {
				sb.append(" ");
			}
		}
		sb.append("#\n");
		return sb.toString();
	}
	
	/**
	 * 获取引擎的结束注释信息
	 * @return 返回引擎的结束注释信息
	 */
	private String getBottomMsg() {
		return "市场风险计算引擎计算结束\n########################################################################################################################\n\n\n\n\n\n\n\n\n\n";
	}
	
	/**
	 * 清空对应表数据
	 * @param db 数据库处理对象
	 * @param tableName 表名
	 * @throws EngineSQLException 数据库操作异常
	 */
	private void clearData(DBConnection db, String tableName) throws EngineSQLException {
		String sql = "";
		if ("oracle".equals(EngineJBO.dbType)) {
			sql = "TRUNCATE TABLE " + tableName;
		} else if ("db2".equals(EngineJBO.dbType)) {
			sql = "alter table " + tableName + " activate not logged initially with empty table";
		}
		db.executeUpdate(sql);
	}
	
	/**
	 * 清空临时表数据(采用alter table的方式进行)
	 * @param db 数据库处理对象
	 * @throws EngineSQLException 数据库操作异常
	 */
	private void clearTempTableData(DBConnection db) throws EngineSQLException {
		// 市场风险标准法风险暴露临时表
		clearData(db, "RWA_ETC_MarketExposureSTD");
		// 利率风险时段结果临时表
		clearData(db, "RWA_ETR_InterestRateTB");
		// 利率风险时区结果临时表
		clearData(db, "RWA_ETR_InterestRateTZ");
		// 利率一般风险结果临时表
		clearData(db, "RWA_ETR_InterestRateGR");
		// 股票风险分类结果临时表
		clearData(db, "RWA_ETR_EquityType");
		// 股票风险结果临时表
		clearData(db, "RWA_ETR_Equity");
		// 外汇风险分类结果临时表
		clearData(db, "RWA_ETR_ExchangeType");
		// 外汇风险结果临时表
		clearData(db, "RWA_ETR_Exchange");
		// 商品风险结果临时表
		clearData(db, "RWA_ETR_Commodity");
		// 市场风险结果临时表
		clearData(db, "RWA_ETR_Market");
		DBUtils.delTableResult(db,"RWA_ERR_MarketExposureSTD", TaskJBO.resultNo);
		DBUtils.delTableResult(db,"RWA_ERR_InterestRateTB", TaskJBO.resultNo);
		DBUtils.delTableResult(db,"RWA_ERR_InterestRateTZ", TaskJBO.resultNo);
		DBUtils.delTableResult(db,"RWA_ERR_InterestRateGR", TaskJBO.resultNo);
		DBUtils.delTableResult(db,"RWA_ERR_EquityType", TaskJBO.resultNo);
		DBUtils.delTableResult(db,"RWA_ERR_Equity", TaskJBO.resultNo);
		DBUtils.delTableResult(db,"RWA_ERR_ExchangeType", TaskJBO.resultNo);
		DBUtils.delTableResult(db,"RWA_ERR_Exchange", TaskJBO.resultNo);
		DBUtils.delTableResult(db,"RWA_ERR_Commodity", TaskJBO.resultNo);
		DBUtils.delTableResult(db,"RWA_ERR_Market", TaskJBO.resultNo);
	}
	
	/**
	 * 根据任务流水号初始化任务信息
	 * @param db 数据库处理对象
	 * @param taskType 任务类型
	 * @param schemeId 计算方案ID
	 * @throws EngineParameterException 参数异常
	 * @throws EngineSQLException 数据库操作异常
	 */
	private void initTaskInfo(DBConnection db, String taskType, String schemeId) throws EngineParameterException, EngineSQLException {
		if ("01".equals(taskType)) {
			// taskType 为 01 表示计算任务
			initGeneralTaskInfo(db, schemeId);
		} else {
			// taskType 为 02 表示即时任务  TODO 暂不支持
			//initImmediateTaskInfo(db, schemeId);
		}
	}
	
	/**
	 * 初始化常规计算任务信息
	 * @param db 数据库处理对象
	 * @param schemeId 计算方案ID
	 * @throws EngineParameterException 参数异常
	 * @throws EngineSQLException 数据库操作异常
	 */
	private void initGeneralTaskInfo(DBConnection db, String schemeId) throws EngineParameterException, EngineSQLException {
		String sql = new StringBuilder().append("SELECT a.SCHEMEID,a.SCHEMENAME,a.RISKAPPROACH,a.MIRRGRAPPROACH,PARAMVERNO ")
				.append(" FROM RWA_EP_SCHEME_B2 a WHERE a.SCHEMEID  = '").append(schemeId).append("'").toString();
		Statement st = null;
		ResultSet rs = null;
		try {
			st = db.createStatement();
			rs = db.executeQuery(st, sql);
			if (rs.next()) {
				TaskJBO.schemeID = rs.getString("SCHEMEID"); // 计算方案流水号
				TaskJBO.schemeName = rs.getString("SCHEMENAME");	// 计算任务名称
				TaskJBO.marketApproach = rs.getString("RISKAPPROACH"); // 市场风险计算方法
				TaskJBO.mirrgrApproach = rs.getString("MIRRGRAPPROACH"); // 利率一般风险计算方法
				TaskJBO.marketParamVerNo = rs.getString("PARAMVERNO"); // 市场风险参数版本流水号
				// 强制结果集走到底
				rs.next();
			} else {
				throw new EngineParameterException("[计算方案ID:" + schemeId + "]设置有误，系统不存在此任务流水号！");
			}
		} catch (SQLException e) {
			throw new EngineSQLException("从数据库中获取计算任务相关信息异常[计算任务流水号:" + schemeId + "]", e);
		} finally {
			db.closeResultSet(rs);
			db.closeStatement(st);
		}
	}
	
	/**
	 * 初始化即时计算任务信息
	 * @param db 数据库处理对象
	 * @param schemeId 任务流水号
	 * @throws EngineParameterException 参数异常
	 * @throws EngineSQLException 数据库操作异常
	 */
/*	private void initImmediateTaskInfo(DBConnection db, String schemeId) throws EngineParameterException, EngineSQLException {
		String sql = "SELECT A.ImTaskName, A.ConsolidateFlag, A.MarketSchemeID, B.RiskApproach, B.MIRRGRApproach, " +
				"A.MarketParamVerNo, A.CreditParamVerNo, A.CRState " +
				" FROM RWA_EP_ImTask A " +
				" LEFT JOIN RWA_EP_Scheme B ON A.MarketSchemeID = B.SchemeID " +
				" WHERE A.ImschemeId = '" + schemeId + "'";
		Statement st = null;
		ResultSet rs = null;
		try {
			st = db.createStatement();
			rs = db.executeQuery(st, sql);
			if (rs.next()) {
				TaskJBO.taskName = rs.getString("ImTaskName");	// 计算任务名称
				TaskJBO.consolidatedFlag = rs.getString("ConsolidateFlag");	// 是否并表
				TaskJBO.schemeID = rs.getString("MarketSchemeID"); // 计算方案流水号
				TaskJBO.marketApproach = rs.getString("RiskApproach"); // 市场风险计算方法
				TaskJBO.mirrgrApproach = rs.getString("MIRRGRApproach"); // 利率一般风险计算方法
				TaskJBO.marketParamVerNo = rs.getString("MarketParamVerNo"); // 市场风险参数版本流水号
				TaskJBO.creditParamVerNo = rs.getString("CreditParamVerNo"); // 信用风险参数版本流水号
				TaskJBO.creditState = rs.getString("CRState"); // 信用风险计算状态
				// 强制结果集走到底
				rs.next();
			} else {
				throw new EngineParameterException("[即时任务流水号:" + schemeId + "]设置有误，系统不存在此任务流水号");
			}
		} catch (SQLException e) {
			throw new EngineSQLException("从数据库中获取即时任务相关信息异常[即时任务流水号:" + schemeId + "]", e);
		} finally {
			db.closeResultSet(rs);
			db.closeStatement(st);
		}
	}*/
	
	/**
	 * 初始化任务相关信息
	 * @param db 数据库处理对象
	 * @throws EngineParameterException 参数异常
	 * @throws EngineSQLException 数据库操作异常
	 */
	private void initTaskOtherInfo(DBConnection db) throws EngineParameterException, EngineSQLException {
		// 通过数据日期初始化计算数据相关信息
		initDataInfo(db, TaskJBO.dataDate);
	}
	
	/**
	 * 根据数据日期初始化计算数据相关信息(通过数据日期获取计算数据相关信息)
	 * @param db 数据库处理对象
	 * @param dataDate 数据日期
	 * @throws EngineSQLException 数据库操作异常
	 * @throws EngineParameterException 参数异常
	 */
	private void initDataInfo(DBConnection db, String dataDate) throws EngineSQLException, EngineParameterException {
		String sql = "SELECT DataNo, MarketConfirmFlag "
				+ "FROM RWA_EL_RISK_B2 WHERE dataNo = '" + StrUtil.removeAll(TaskJBO.dataDate,'-') + "'";
		Statement st = null;
		ResultSet rs = null;
		try {
			st = db.createStatement();
			rs = db.executeQuery(st, sql);
			if (rs.next()) {
				TaskJBO.dataNo = rs.getString("DataNo");
				TaskJBO.confirmFlag = rs.getString("MarketConfirmFlag");
				// 强制结果集走到底
				rs.next();
			} else {
				throw new EngineParameterException("未找到[数据日期:" + dataDate + "]关联计算数据信息");
			}
		} catch (SQLException e) {
			throw new EngineSQLException("结果集获取计算数据相关信息异常[数据日期:" + dataDate + "]", e);
		} finally {
			db.closeResultSet(rs);
			db.closeStatement(st);
		}
	}
	
	/**
	 * 计算相关参数校验
	 * @throws EngineParameterException 参数异常
	 */
	private void checkTaskParams() throws EngineParameterException {
		if (!"1".equals(TaskJBO.confirmFlag)) {
			throw new EngineParameterException("数据[" +TaskJBO.dataNo + "]未确认");
		}
		if (StrUtil.isBlank(TaskJBO.schemeID)) {
			throw new EngineParameterException("[计算方案ID设置有误");
		}
		if (StrUtil.isBlank(TaskJBO.marketParamVerNo)) {
			throw new EngineParameterException("[市场风险参数版本流水号设置有误");
		}
/*		if (TaskJBO.creditParamVerNo == null || "".equals(TaskJBO.creditParamVerNo)) {
			throw new EngineParameterException("[信用风险参数版本流水号设置有误");
		}*/
		if (!"0".equals(TaskJBO.consolidatedFlag) && !"1".equals(TaskJBO.consolidatedFlag)) {
			throw new EngineParameterException("[是否并表:" + TaskJBO.consolidatedFlag + "]设置有误");
		}
		if (TaskJBO.marketApproach == null 
				|| (!"01".equals(TaskJBO.marketApproach) && !"02".equals(TaskJBO.marketApproach))) {
			throw new EngineParameterException("[市场风险计算方法:" + TaskJBO.marketApproach + "]设置有误");
		}
		if (TaskJBO.mirrgrApproach == null || 
				(!"010101".equals(TaskJBO.mirrgrApproach) && !"010102".equals(TaskJBO.mirrgrApproach))) {
			throw new EngineParameterException("[利率一般风险计算方法 :" + TaskJBO.mirrgrApproach + "]设置有误");
		}
	}

}
