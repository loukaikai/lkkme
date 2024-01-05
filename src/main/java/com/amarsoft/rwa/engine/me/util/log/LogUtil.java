package com.amarsoft.rwa.engine.me.util.log;

import cn.hutool.core.util.StrUtil;
import com.amarsoft.rwa.engine.me.exception.EngineSQLException;
import com.amarsoft.rwa.engine.me.jbo.EngineJBO;
import com.amarsoft.rwa.engine.me.jbo.TaskJBO;
import com.amarsoft.rwa.engine.me.step.StepType;
import com.amarsoft.rwa.engine.me.util.EngineUtil;
import com.amarsoft.rwa.engine.me.util.db.DBConnection;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.text.ParseException;
import java.util.Date;

/**
 * 数据库日志操作
 * @author 陈庆
 * @version 1.0 2013-06-05
 */
@Slf4j
public class LogUtil {
	/**
	 * 插入任务日志
	 * @param db 数据库处理对象
	 * @throws EngineSQLException 数据库操作异常
	 * @throws ParseException 字符串转日期格式异常
	 */
	public static void insertTaskDetailLog(DBConnection db) throws EngineSQLException, ParseException {
		String sql = "INSERT INTO RWA_EL_TaskDetail(LogSerialNo, RiskType, TaskType, TaskID, " +
				"ResultSerialNo, DataDate, StartTime, CalculationState) VALUES(?,?,?,?,?,?,?,?)";
		PreparedStatement pst = db.prepareStatement(sql);
		try {
			pst.setString(1, TaskJBO.logID);
			pst.setString(2, EngineJBO.riskType);
			pst.setString(3, TaskJBO.taskType);
			pst.setString(4, TaskJBO.schemeID);
			pst.setString(5, TaskJBO.resultNo);
			pst.setDate(6, EngineUtil.getFormatSqlDate(TaskJBO.dataDate)); // 数据日期
			pst.setString(7, EngineUtil.getFormatDate(TaskJBO.startTime));
			pst.setString(8, "02");
			// 任务运行中
			pst.execute();
		} catch (SQLException e) {
			throw new EngineSQLException("插入任务日志异常[" + sql + "][日志ID："
					+ TaskJBO.logID + "]", e);
		} finally {
			db.closePrepareStatement(pst);
		}
	}
	
	/**
	 * 初始化计算各步骤日志
	 * @param db 数据库处理对象
	 * @throws EngineSQLException 数据库操作异常
	 */
	public static void initStepLog(DBConnection db) throws EngineSQLException {
		String sql = "INSERT INTO RWA_EL_Step(LogSerialNo, RiskType, CalculationStep, " +
				"TaskID, ResultSerialNo, StepState) VALUES(?,?,?,?,?,?)";
		PreparedStatement pst = db.prepareStatement(sql);
		try {
			insertStepLogByPst(pst, StepType.LOADDATA.getId());
			insertStepLogByPst(pst, StepType.MAPPINGPARAMS.getId());
			insertStepLogByPst(pst, StepType.CALCULATERWA.getId());
			insertStepLogByPst(pst, StepType.INSERTRESULT.getId());
			db.executeBatch(pst);
		} finally {
			db.closePrepareStatement(pst);
		}
	}
	
	/**
	 * 插入相应步骤的步骤初始日志
	 * @param pst 初始化步骤日志的预处理对象
	 * @param stepID 步骤ID
	 * @throws EngineSQLException 数据库操作异常
	 */
	private static void insertStepLogByPst(PreparedStatement pst, String stepID) throws EngineSQLException {
		try {
			pst.setString(1, TaskJBO.logID);
			pst.setString(2, EngineJBO.riskType);
			pst.setString(3, stepID);
			pst.setString(4, TaskJBO.schemeID);
			pst.setString(5, TaskJBO.resultNo);
			pst.setString(6, "01");
			// 初始化，未运行
			pst.addBatch();
		} catch (SQLException e) {
			throw new EngineSQLException("步骤日志PST插入日志异常[步骤ID：" + stepID + "]", e);
		}
	}
	
	/**
	 * 记录步骤开始日志
	 * @param db 数据库处理对象
	 * @throws EngineSQLException 数据库操作异常
	 */
	public static void updateStepLogByStart(DBConnection db) throws EngineSQLException {
		String sql = "UPDATE RWA_EL_Step SET StartTime=?, StepState=? WHERE LogSerialNo=? AND CalculationStep=?";
		PreparedStatement pst = db.prepareStatement(sql);
		try {
			pst.setString(1, EngineUtil.getFormatDate(TaskJBO.currentStepTime));
			// 步骤开始运行
			pst.setString(2, "02");
			pst.setString(3, TaskJBO.logID);
			pst.setString(4, TaskJBO.currentStep.getId());
			pst.execute();
		} catch (SQLException e) {
			throw new EngineSQLException("更新步骤日志异常[" + sql + "][日志ID：" + TaskJBO.logID + 
					"][步骤ID：" + TaskJBO.currentStep.getId() + "]", e);
		} finally {
			db.closePrepareStatement(pst);
		}
	}
	
	/**
	 * 记录步骤正常结束日志
	 * @param db 数据库处理对象
	 * @throws EngineSQLException 数据库操作异常
	 */
	public static void updateStepLogByOver(DBConnection db) throws EngineSQLException {
		String sql = "UPDATE RWA_EL_Step SET EndTime=?, CalculationTime=?, TimeConsumed=?, StepState=? " +
				" WHERE LogSerialNo=? AND CalculationStep=?";
		PreparedStatement pst = db.prepareStatement(sql);
		try {
			Date d = new Date();
			long t = d.getTime() - TaskJBO.currentStepTime.getTime();
			pst.setString(1, EngineUtil.getFormatDate(d));
			pst.setString(2, EngineUtil.changeTimeToString(t));
			pst.setLong(3, t);
			// 步骤正常结束，完成
			pst.setString(4, "03");
			pst.setString(5, TaskJBO.logID);
			pst.setString(6, TaskJBO.currentStep.getId());
			pst.execute();
		} catch (SQLException e) {
			throw new EngineSQLException("更新步骤日志异常[" + sql + "][日志ID：" + TaskJBO.logID + 
					"][步骤ID：" + TaskJBO.currentStep.getId() + "]", e);
		} finally {
			db.closePrepareStatement(pst);
		}
	}
	
	/**
	 * 记录步骤异常结束日志
	 * @param db 数据库处理对象
	 * @param t 异常类
	 * @throws EngineSQLException 数据库操作异常
	 */
	public static void updateStepLogByException(DBConnection db, Throwable t) throws EngineSQLException {
		updateStepLogByException(db, EngineUtil.getExceptionInfo(t));
	}
	
	/**
	 * 记录步骤异常结束日志
	 * @param db 数据库处理对象
	 * @param msg 异常信息
	 * @throws EngineSQLException 数据库操作异常
	 */
	public static void updateStepLogByException(DBConnection db, String msg) throws EngineSQLException {
		String sql = "UPDATE RWA_EL_Step SET EndTime=?, CalculationTime=?, TimeConsumed=?, StepState=?, ExceptionDescrip=? " +
				" WHERE LogSerialNo=? AND CalculationStep=?";
		PreparedStatement pst = null;
		// qzf 20231019 防止异常日志太大，截取2000长度
		try {
			if(StrUtil.isNotEmpty(msg)){
				msg = StrUtil.subWithLength(msg,0,2000);
			}
		} catch (Exception e){
			msg = "异常日志记录异常，请通过后台日志查看";
		}
		try {
			pst = db.prepareStatement(sql);
			long time = TaskJBO.overTime.getTime() - TaskJBO.currentStepTime.getTime();
			pst.setString(1, EngineUtil.getFormatDate(TaskJBO.overTime));
			pst.setString(2, EngineUtil.changeTimeToString(time));
			pst.setLong(3, time);
			// 步骤异常结束，失败
			pst.setString(4, "04");
			pst.setString(5, msg);
			pst.setString(6, TaskJBO.logID);
			pst.setString(7, TaskJBO.currentStep.getId());
			pst.execute();
		} catch (SQLException e) {
			throw new EngineSQLException("更新步骤日志异常[" + sql + "][日志ID：" + TaskJBO.logID + 
					"][步骤ID：" + TaskJBO.currentStep.getId() + "]", e);
		} finally {
			db.closePrepareStatement(pst);
		}
	}
	
	/**
	 * 更新任务结束日志
	 * @param db 数据库处理对象
	 * @throws EngineSQLException 数据库处理异常
	 */
	public static void updateTaskDetailLog(DBConnection db, String calculationState) throws EngineSQLException {
		String sql = "UPDATE RWA_EL_TaskDetail SET DataNo=?, EndTime=?, CalculationTime=?, TimeConsumed=?, CalculationState=? " +
				" WHERE LogSerialNo=? AND RiskType=?";
		PreparedStatement pst = null;
		try {
			pst = db.prepareStatement(sql);
			if (TaskJBO.dataNo == null) {
				pst.setNull(1, Types.VARCHAR);
			} else {
				pst.setString(1, TaskJBO.dataNo);
			}
			long time = TaskJBO.overTime.getTime() - TaskJBO.startTime.getTime();
			pst.setString(2, EngineUtil.getFormatDate(TaskJBO.overTime));
			pst.setString(3, EngineUtil.changeTimeToString(time));
			pst.setLong(4, time);
			pst.setString(5, calculationState);
			pst.setString(6, TaskJBO.logID);
			pst.setString(7, EngineJBO.riskType);
			pst.execute();
		} catch (SQLException e) {
			throw new EngineSQLException("更新任务日志异常[" + sql + "][日志ID："
					+ TaskJBO.logID + "]", e);
		} finally {
			db.closePrepareStatement(pst);
		}
	}

	/**
	 * 清理缓存日志
	 * @param db
	 * @param resultNo
	 * @param riskType
	 * @throws EngineSQLException
	 */
	public static void delTaskLog(DBConnection db, String resultNo, String riskType) throws EngineSQLException {
		db.executeUpdate("delete from RWA_EL_TaskDetail where ResultSerialNo ='" + resultNo + "' and RISKTYPE = '" + riskType + "'");
		db.executeUpdate("delete from RWA_EL_Step where ResultSerialNo ='" + resultNo + "' and RISKTYPE = '" + riskType + "'");
	}

	/**
	 * 查询市场风险引擎跑批状态
	 * @param resultNo
	 * @param riskType
	 * @return
	 */
	public static String queryCalculationState(String resultNo, String riskType){
		String calculationState = "22";
		String sql = "select CASE WHEN calculationstate = '01' THEN '10' " +
				" WHEN calculationstate = '02' THEN '11' " +
				" WHEN calculationstate = '03' THEN '21' " +
				" WHEN calculationstate = '04' THEN '29' " +
				" ELSE 22 END AS calculationstate from RWA_EL_TaskDetail where ResultSerialNo ='"
				+ resultNo + "' and RISKTYPE = '" + riskType + "'";
		DBConnection db = new DBConnection();
		Statement st = null;
		ResultSet rs = null;
		try {
			db.createConnection();
			st = db.createStatement();
			rs = db.executeQuery(st, sql);
			if (rs.next()) {
				calculationState = rs.getString("calculationstate");
			}
		} catch (Exception e) {
			log.error("查询市场风险状态异常，异常原因：",e);
		} finally {
			db.closeResultSet(rs);
			db.closeStatement(st);
			db.close();
		}
		return calculationState;
	}

	
}
