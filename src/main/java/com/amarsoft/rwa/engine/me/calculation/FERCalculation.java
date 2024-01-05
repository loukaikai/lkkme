package com.amarsoft.rwa.engine.me.calculation;

import com.amarsoft.rwa.engine.me.exception.EngineSQLException;
import com.amarsoft.rwa.engine.me.jbo.ConstantJBO;
import com.amarsoft.rwa.engine.me.jbo.TaskJBO;
import com.amarsoft.rwa.engine.me.util.EngineUtil;
import com.amarsoft.rwa.engine.me.util.db.DBConnection;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;

/**
 * 外汇风险计算
 * @author 陈庆
 * @version 1.0 2015-09-08
 *
 */
public class FERCalculation implements RWACalculation {

	/**
	 * 执行计算
	 */
	public void calculate() throws Exception {
		DBConnection db = new DBConnection();
		try {
			// 创建数据库连接
			db.createConnection();
			// 计算得尔塔加权头寸
			// 期权基础工具，得尔塔法 : 头寸 * 得尔塔值 
			// 其他： 头寸
			this.updateDeltaPosition(db);
			
			// 头寸调整
			// delta可能为负，则相应的需要转换
			this.adjustDeltaPosition(db);
			//更新栏位是10和11的数据，需要特殊处理
			this.updateExchangeRiskType(db);
			
			// 插入外汇风险分类结果
			this.insertExchangeType(db);
			
			// 获取外汇风险结果
			Double[] er = this.getExchangeResult(db);
			
			// 计算并插入外汇风险结果
			this.insertExchangeResult(db, er);
		} finally {
			db.close();
		}
	}
	
	/**
	 * 计算得尔塔加权头寸
	 * @param db 数据库处理对象
	 * @return 更新记录数
	 * @throws EngineSQLException 数据库操作异常
	 */
	private int updateDeltaPosition(DBConnection db) throws EngineSQLException {
		// 简易法无需在这边计算RWA
		String sql = "UPDATE RWA_ETC_MarketExposureSTD SET DeltaPosition = " +
				"(CASE WHEN OptionUnderlyingFlag = '1' AND MORApproach = '010502' THEN Position * Delta ELSE Position END) " +
				"WHERE MarketRiskType = '03' And OptionSimpleFlag = '0'";
		return db.executeUpdate(sql);
	}
	
	/**
	 * 调整得尔塔加权头寸
	 * <br>若计算后的得尔塔加权头寸为负，则改为正，同时头寸属性改为相对方向
	 * @param db 数据库处理对象
	 * @return 更新记录数
	 * @throws EngineSQLException 数据库操作异常
	 */
	private int adjustDeltaPosition(DBConnection db) throws EngineSQLException {
		String sql = "UPDATE RWA_ETC_MarketExposureSTD SET DeltaPosition = -DeltaPosition, " +
				"PositionType = (CASE WHEN PositionType = '01' THEN '02' ELSE '01' END) " +
				"WHERE MarketRiskType = '03' And OptionSimpleFlag = '0' AND DeltaPosition < 0";
		return db.executeUpdate(sql);
	}

	/**
	 * 更新栏位是10和11的数据，需要特殊处理
	 * @param db
	 * @return
	 * @throws EngineSQLException
	 */
	private int updateExchangeRiskType(DBConnection db) throws EngineSQLException {
		String sql = "  MERGE INTO RWA_ETC_MARKETEXPOSURESTD M " +
				" USING ( SELECT a.ORGTYPE,a.CURRENCY,sum(CASE WHEN (a.POSITIONTYPE = '01') THEN a.POSITION ELSE -a.POSITION END ) AS GNP " +
				" FROM RWA_ETC_MARKETEXPOSURESTD a WHERE MarketRiskType = '03' And OptionSimpleFlag = '0' AND (EXCHANGERISKTYPE IN ('10','11') OR EXCHANGERISKTYPE IS NULL) " +
				" GROUP BY a.ORGTYPE,a.CURRENCY ) N ON (M.ORGTYPE = N.ORGTYPE AND M.CURRENCY = N.CURRENCY) " +
				" WHEN MATCHED THEN UPDATE SET M.EXCHANGERISKTYPE = (CASE WHEN N.GNP < 0 THEN '11' ELSE '10' END) " +
				" WHERE M.MarketRiskType = '03' And M.OptionSimpleFlag = '0' AND (M.EXCHANGERISKTYPE IN ('10','11') OR M.EXCHANGERISKTYPE IS NULL) ";
		return db.executeUpdate(sql);
	}

	/**
	 * 插入外汇风险分类结果
	 * @param db 数据库处理对象
	 * @return 更新记录数
	 * @throws EngineSQLException 数据库操作异常
	 */
	private int insertExchangeType(DBConnection db) throws EngineSQLException {
		String sql = "INSERT INTO RWA_ETR_ExchangeType" +
				"(ExchangeRiskType, OrgType, ResultSerialNo, DataDate, DataNo, " +
				"GGLP, GGSP, GNP, GOLP, GOSP, ONP, GSLP, GSSP, SNP, GNSNP) " +
				"SELECT ExchangeRiskType, OrgType, '" + TaskJBO.resultNo +
				"' AS ResultSerialNo, TO_DATE('" + TaskJBO.dataDate + 
				"','YYYY-MM-DD') AS DataDate, '" + TaskJBO.dataNo + "' AS DataNo, " + 
				"SUM(CASE WHEN StructuralExpoFlag = '0' AND OptionUnderlyingFlag = '0' " +
				" AND PositionType = '01' THEN DeltaPosition ELSE 0 END) AS GGLP, " +
				"SUM(CASE WHEN StructuralExpoFlag = '0' AND OptionUnderlyingFlag = '0' " +
				" AND PositionType = '02' THEN DeltaPosition ELSE 0 END) AS GGSP, " +
				"SUM(CASE WHEN StructuralExpoFlag = '0' AND OptionUnderlyingFlag = '0' " +
				" AND PositionType = '01' THEN DeltaPosition " +
				" WHEN StructuralExpoFlag = '0' AND OptionUnderlyingFlag = '0' " +
				" AND PositionType = '02' THEN -DeltaPosition ELSE 0 END) AS GNP, " +
				"SUM(CASE WHEN StructuralExpoFlag = '0' AND OptionUnderlyingFlag = '1' " +
				" AND PositionType = '01' THEN DeltaPosition ELSE 0 END) AS GOLP, " +
				"SUM(CASE WHEN StructuralExpoFlag = '0' AND OptionUnderlyingFlag = '1' " +
				" AND PositionType = '02' THEN DeltaPosition ELSE 0 END) AS GOSP, " +
				"SUM(CASE WHEN StructuralExpoFlag = '0' AND OptionUnderlyingFlag = '1' " +
				" AND PositionType = '01' THEN DeltaPosition " +
				" WHEN StructuralExpoFlag = '0' AND OptionUnderlyingFlag = '1' " +
				" AND PositionType = '02' THEN -DeltaPosition ELSE 0 END) AS ONP, " +
				"SUM(CASE WHEN StructuralExpoFlag = '1' AND PositionType = '01' THEN DeltaPosition ELSE 0 END) AS GSLP, " +
				"SUM(CASE WHEN StructuralExpoFlag = '1' AND PositionType = '02' THEN DeltaPosition ELSE 0 END) AS GSSP, " +
				"SUM(CASE WHEN StructuralExpoFlag = '1' AND PositionType = '01' THEN DeltaPosition " +
				" WHEN StructuralExpoFlag = '1' AND PositionType = '02' THEN -DeltaPosition ELSE 0 END) AS SNP, " +
				"SUM(CASE WHEN StructuralExpoFlag = '0' AND PositionType = '01' THEN DeltaPosition " +
				" WHEN StructuralExpoFlag = '0' AND PositionType = '02' THEN -DeltaPosition ELSE 0 END) AS GNSNP " +
				"FROM RWA_ETC_MarketExposureSTD " +
				"WHERE MarketRiskType = '03' AND OptionSimpleFlag = '0' " +
				"GROUP BY ExchangeRiskType, OrgType";
		return db.executeUpdate(sql);
	}
	
	/**
	 * 获取外汇风险头寸汇总结果
	 * <br>其中: [0]为外币资产组合净多头总额,[1]为外币资产组合净空头总额,[2]为黄金净头寸
	 * @param db 数据库处理对象
	 * @return 返回外汇风险头寸汇总结果
	 * @throws EngineSQLException 数据库操作异常
	 */
	private Double[] getExchangeResult(DBConnection db) throws EngineSQLException {
		Double[] er = {0.0, 0.0, 0.0};
		// ExchangeRiskType = '12' 黄金
		String sql = "SELECT SUM(CASE WHEN ExchangeRiskType = '12' THEN 0 WHEN GNSNP > 0 THEN GNSNP ELSE 0 END) AS GFCNLP, " +
				"SUM(CASE WHEN ExchangeRiskType = '12' THEN 0 WHEN GNSNP < 0 THEN -GNSNP ELSE 0 END) AS GFCNSP, " +
				"SUM(CASE WHEN ExchangeRiskType = '12' THEN GNSNP ELSE 0 END) AS GoldNP " +
				"FROM RWA_ETR_ExchangeType";
		Statement st = null;
		ResultSet rs = null;
		try {
			st = db.createStatement();
			rs = db.executeQuery(st, sql);
			if (rs.next()) {
				er[0] = rs.getDouble("GFCNLP"); // 外币资产组合净多头总额
				er[1] = rs.getDouble("GFCNSP"); // 外币资产组合净空头总额
				er[2] = rs.getDouble("GoldNP"); // 黄金净头寸
				// 强制循环到底
				while (rs.next());
			}
		} catch (SQLException e) {
			throw new EngineSQLException("结果集获取外汇风险分类结果临时表数据异常" , e);
		} finally {
			db.closeResultSet(rs);
			db.closeStatement(st);
		}
		return er;
	}
	
	/**
	 * 根据头寸结果，计算外汇风险资本要求，并插入结果
	 * @param db 数据库处理对象
	 * @param er 外汇风险头寸汇总结果
	 * @return 返回插入的记录数(1)
	 * @throws EngineSQLException 数据库操作异常
	 * @throws ParseException 日期转换异常
	 */
	private int insertExchangeResult(DBConnection db, Double[] er) throws EngineSQLException, ParseException {
		// 计算外汇风险资本要求
		double gfcp = Math.max(er[0], er[1]);
		double gnep = gfcp + Math.abs(er[2]);
		double pr = ConstantJBO.FERCRPR;
		double rc = gnep * pr;
		TaskJBO.FERGRCR = rc;
		// 插入结果
		String sql = "INSERT INTO RWA_ETR_Exchange" +
				"(ResultSerialNo, DataDate, DataNo, GFCNLP, GFCNSP, GFCP, GoldNP, GNEP, PR, RC) " +
				"VALUES(?,?,?,?,?,?,?,?,?,?)";
		PreparedStatement pst = null;
		try {
			pst = db.prepareStatement(sql);
			pst.setString(1, TaskJBO.resultNo);
			pst.setDate(2, EngineUtil.getFormatSqlDate(TaskJBO.dataDate));
			pst.setString(3, TaskJBO.dataNo);
			pst.setDouble(4, er[0]);
			pst.setDouble(5, er[1]);
			pst.setDouble(6, gfcp);
			pst.setDouble(7, er[2]);
			pst.setDouble(8, gnep);
			pst.setDouble(9, pr);
			pst.setDouble(10, rc);
			pst.execute();
		} catch (SQLException e) {
			throw new EngineSQLException("插入外汇风险结果异常" , e);
		} finally {
			db.closePrepareStatement(pst);
		}
		return 1;
	}

}
