package com.amarsoft.rwa.engine.me.calculation;

import com.amarsoft.rwa.engine.me.exception.EngineSQLException;
import com.amarsoft.rwa.engine.me.jbo.ConstantJBO;
import com.amarsoft.rwa.engine.me.jbo.TaskJBO;
import com.amarsoft.rwa.engine.me.util.db.DBConnection;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 期权风险计算
 * @author 陈庆
 * @version 1.0 2015-09-08
 *
 */
public class ORCalculation implements RWACalculation {

	/**
	 * 执行计算
	 */
	public void calculate() throws Exception {
		DBConnection db = new DBConnection();
		try {
			// 创建数据库连接
			db.createConnection();
			
			/*
			 *  简易法计算
			 */
			// 确定简易法计算场景
			this.updateOptionSimpleScene(db);
			
			// 计算简易法资本要求
			this.calculateCROptionSimple(db);
			
			/*
			 * delte+法计算
			 */
			// 计算伽马(Gamma)风险的资本要求
			this.calculateGammaCR(db);
			
			// 维加(vega)风险的资本要求
			this.calculateVegaCR(db);
			
			// 统计期权风险结果
			this.statisticalResult(db);
		} finally {
			db.close();
		}
	}
	
	/**
	 * 确定简易法计算场景
	 * @param db 数据库处理对象
	 * @return 更新记录数
	 * @throws EngineSQLException 数据库操作异常
	 */
	private int updateOptionSimpleScene(DBConnection db) throws EngineSQLException {
		// HedgeSpotFlag = '1' 存在对冲现货，则计算场景为 存在基础工具对冲，否则为 只存在期权多头
		String sql = "UPDATE RWA_ETC_MarketExposureSTD SET OptionSimpleScene = " +
				"(CASE WHEN HedgeSpotFlag = '1' THEN '01050101' ELSE '01050102' END) " +
				"WHERE OptionUnderlyingFlag = '1' And OptionSimpleFlag = '1'";
		return db.executeUpdate(sql);
	}
	
	/**
	 * 简易法计算场景 计算资本要求
	 * @param db 数据库处理对象
	 * @return 更新记录数
	 * @throws EngineSQLException 数据库操作异常
	 */
	private int calculateCROptionSimple(DBConnection db) throws EngineSQLException {
		int r = 0;
		// 存在基础工具对冲
		r = r + this.calculateCROptionSimple1(db);
		// 只存在期权多头
		r = r + this.calculateCROptionSimple2(db);
		return r;
	}
	
	/**
	 * 简易法计算场景-存在基础工具对冲 计算资本要求
	 * @param db 数据库处理对象
	 * @return 更新记录数
	 * @throws EngineSQLException 数据库操作异常
	 */
	private int calculateCROptionSimple1(DBConnection db) throws EngineSQLException {
		// 资本要求等于基础工具的市场价值乘以(特定市场风险和一般市场风险)资本要求比率之和，再减去期权溢价。资本要求最低为零
		String sql = "UPDATE RWA_ETC_MarketExposureSTD SET OptionSimpleCR = " +
				"(CASE WHEN Position * (SRPR + GRPR) > OptionPremium THEN Position * (SRPR + GRPR) - OptionPremium ELSE 0 END)" +
				"WHERE OptionUnderlyingFlag = '1' And OptionSimpleFlag = '1' AND OptionSimpleScene = '01050101'";
		return db.executeUpdate(sql);
	}
	
	/**
	 * 简易法计算场景-只存在期权多头 计算资本要求
	 * @param db 数据库处理对象
	 * @return 更新记录数
	 * @throws EngineSQLException 数据库操作异常
	 */
	private int calculateCROptionSimple2(DBConnection db) throws EngineSQLException {
		// 资本要求等于基础工具的市场价值乘以(特定市场风险和一般市场风险)资本要求比率之和与期权的市场价值两者中的较小者
		String sql = "UPDATE RWA_ETC_MarketExposureSTD SET OptionSimpleCR = " +
				"(CASE WHEN Position * (SRPR + GRPR) < OptionValue THEN Position * (SRPR + GRPR) ELSE OptionValue END)" +
				"WHERE OptionUnderlyingFlag = '1' And OptionSimpleFlag = '1' AND OptionSimpleScene = '01050102'";
		return db.executeUpdate(sql);
	}
	
	/**
	 * 计算基础工具的 伽马值和 与 维加值和
	 * @param db 数据库处理对象
	 * @return 更新记录数
	 * @throws EngineSQLException 数据库操作异常
	 */
	private int calculateGammaCR(DBConnection db) throws EngineSQLException {
		// DB2对于多个number乘除有一定的限制，只能拆分
		// VU = 头寸 * 一般风险计提比率， 暂存GammaSum
		String sql = "UPDATE RWA_ETC_MarketExposureSTD SET GammaSum = " +
				"Position * GRPR " +
				"WHERE OptionUnderlyingFlag = '1' And MORApproach = '010502'";
		db.executeUpdate(sql);
		// (VU)2
		sql = "UPDATE RWA_ETC_MarketExposureSTD SET GammaSum = GammaSum * GammaSum " +
				"WHERE OptionUnderlyingFlag = '1' And MORApproach = '010502'";
		db.executeUpdate(sql);
		// 伽马效应值＝0.5×Gamma×(VU)2， 暂存GammaSum
		sql = "UPDATE RWA_ETC_MarketExposureSTD SET GammaSum = " +
				"0.5 * Gamma * GammaSum " +
				"WHERE OptionUnderlyingFlag = '1' And MORApproach = '010502'";
		db.executeUpdate(sql);
		
		// TODO 同一基础工具每项期权汇总：可改为merge into
		// 当前同一基础工具的各项期权认定 为 同一基础工具类型，非同一基础工具ID
		// 同一基础工具每项期权对应的伽马效应值相加得出每一基础工具的净伽马效应值
		// 同时计算维加值和
		// 对于基础工具为债券的，同一基础工具认定条件暂定为“基础工具名称+金融工具ID”相同
		sql = "UPDATE RWA_ETC_MarketExposureSTD t1 SET (t1.NetGammaEffect, t1.VegaSum) = " +
				"(SELECT t2.NetGammaEffect, t2.VegaSum FROM " +
				" (SELECT OptionUnderlyingName, InstrumentsID, SUM(GammaSum) AS NetGammaEffect, SUM(Vega) AS VegaSum " +
				" FROM RWA_ETC_MarketExposureSTD WHERE OptionUnderlyingFlag = '1' And MORApproach = '010502' "
				+ "And OptionRiskType = '01' And InteRateRiskType = '01' " +
				" GROUP BY OptionUnderlyingName, InstrumentsID) t2 "
				+ "WHERE t2.OptionUnderlyingName = t1.OptionUnderlyingName And t2.InstrumentsID = t1.InstrumentsID) " +
				"WHERE t1.OptionUnderlyingFlag = '1' And t1.MORApproach = '010502' "
				+ "And OptionRiskType = '01' And InteRateRiskType = '01'";
		// 对于基础工具为利率的，同一基础工具认定条件暂定为“基础工具名称+到期日法时段”相同
		sql = "UPDATE RWA_ETC_MarketExposureSTD t1 SET (t1.NetGammaEffect, t1.VegaSum) = " +
				"(SELECT t2.NetGammaEffect, t2.VegaSum FROM " +
				" (SELECT OptionUnderlyingName, MaturityTimeBand, SUM(GammaSum) AS NetGammaEffect, SUM(Vega) AS VegaSum " +
				" FROM RWA_ETC_MarketExposureSTD WHERE OptionUnderlyingFlag = '1' And MORApproach = '010502' "
				+ "And OptionRiskType = '01' And InteRateRiskType = '02' " +
				" GROUP BY OptionUnderlyingName, MaturityTimeBand) t2 "
				+ "WHERE t2.OptionUnderlyingName = t1.OptionUnderlyingName And t2.MaturityTimeBand = t1.MaturityTimeBand) " +
				"WHERE t1.OptionUnderlyingFlag = '1' And t1.MORApproach = '010502' "
				+ "And OptionRiskType = '01' And InteRateRiskType = '02'";
		// 对于基础工具为其他的，同一基础工具认定条件暂定为“基础工具名称”相同
		sql = "UPDATE RWA_ETC_MarketExposureSTD t1 SET (t1.NetGammaEffect, t1.VegaSum) = " +
				"(SELECT t2.NetGammaEffect, t2.VegaSum FROM (" +
				" SELECT OptionUnderlyingName, SUM(GammaSum) AS NetGammaEffect, SUM(Vega) AS VegaSum " +
				" FROM RWA_ETC_MarketExposureSTD WHERE OptionUnderlyingFlag = '1' And MORApproach = '010502' " +
				" GROUP BY OptionUnderlyingName) t2 WHERE t2.OptionUnderlyingName = t1.OptionUnderlyingName) " +
				"WHERE t1.OptionUnderlyingFlag = '1' And t1.MORApproach = '010502' And t1.GammaSum IS NULL ";
		db.executeUpdate(sql);
		
		// 0.5×Gamma×(VU)2 (和) ≠ 0.5×Gamma(和)×(VU)2 
		// 仅当基础工具的净伽马效应值为负值时，才须计算相应的资本要求，且资本要求总额等于这些净伽马效应值之和的绝对值
		// 伽马效应值 GammaSum
		sql = "UPDATE RWA_ETC_MarketExposureSTD SET GammaCR = " +
				"CASE WHEN NetGammaEffect < 0 THEN -GammaSum ELSE 0 END " +
				"WHERE OptionUnderlyingFlag = '1' And MORApproach = '010502'";
		return db.executeUpdate(sql);
	}
	
	/**
	 * 计算基础工具的 伽马值和 与 维加值和
	 * @param db 数据库处理对象
	 * @return 更新记录数
	 * @throws EngineSQLException 数据库操作异常
	 */
	private int calculateVegaCR(DBConnection db) throws EngineSQLException {
		String sql = "";
		// 基础工具维加风险的资本要求＝25%×该基础工具波动率×|该基础工具的各项期权的维加值之和|
		sql = "UPDATE RWA_ETC_MarketExposureSTD SET VegaCR = " +
				"(CASE WHEN VegaSum < 0 THEN -VegaSum ELSE VegaSum END) * 0.25 * Volatility " +
				"WHERE OptionUnderlyingFlag = '1' And MORApproach = '010502'";
		return db.executeUpdate(sql);
	}
	
	/**
	 * 统计期权风险各类型结果
	 * @param db 数据库处理对象
	 * @throws EngineSQLException 数据库操作异常
	 */
	private void statisticalResult(DBConnection db) throws EngineSQLException {
		String sql = " SELECT SUM(CASE WHEN OptionSimpleScene = '01050101' AND OptionRiskType = '01' THEN OptionSimpleCR " +
				"  WHEN OptionSimpleScene = '01050102' AND OptionRiskType = '01' THEN OptionSimpleCR " +
				"  WHEN MORApproach = '010502' AND OptionRiskType = '01' THEN GammaCR + VegaCR ELSE 0 END) AS ODIROCR, " +
				" SUM(CASE WHEN OptionSimpleScene = '01050101' AND OptionRiskType = '02' THEN OptionSimpleCR " +
				"  WHEN OptionSimpleScene = '01050102' AND OptionRiskType = '02' THEN OptionSimpleCR " +
				"  WHEN MORApproach = '010502' AND OptionRiskType = '02' THEN GammaCR + VegaCR ELSE 0 END) AS ODEOCR," +
				" SUM(CASE WHEN OptionSimpleScene = '01050101' AND OptionRiskType = '03' THEN OptionSimpleCR " +
				"  WHEN OptionSimpleScene = '01050102' AND OptionRiskType = '03' THEN OptionSimpleCR " +
				"  WHEN MORApproach = '010502' AND OptionRiskType = '03' THEN GammaCR + VegaCR ELSE 0 END) AS ODFEOCR," +
				" SUM(CASE WHEN OptionSimpleScene = '01050101' AND OptionRiskType = '04' THEN OptionSimpleCR " +
				"  WHEN OptionSimpleScene = '01050102' AND OptionRiskType = '04' THEN OptionSimpleCR " +
				"  WHEN MORApproach = '010502' AND OptionRiskType = '04' THEN GammaCR + VegaCR ELSE 0 END) AS ODCOCR" +
				" FROM RWA_ETC_MarketExposureSTD WHERE OptionUnderlyingFlag = '1' ";
		Statement st = null;
		ResultSet rs = null;
		try {
			st = db.createStatement();
			rs = db.executeQuery(st, sql);
			if (rs.next()) {
				//TaskJBO.OSHSCR = rs.getDouble("OSHSCR");
				//TaskJBO.OSLPCR = rs.getDouble("OSLPCR");
				TaskJBO.ODIROCR = rs.getDouble("ODIROCR");
				TaskJBO.ODEOCR = rs.getDouble("ODEOCR");
				TaskJBO.ODFEOCR = rs.getDouble("ODFEOCR");
				TaskJBO.ODCOCR = rs.getDouble("ODCOCR");
				//汇总需要 * 对应系数
				TaskJBO.ORGRCR = TaskJBO.ODIROCR * ConstantJBO.IRR_RF + TaskJBO.ODEOCR * ConstantJBO.ER_RF
						+ TaskJBO.ODFEOCR * ConstantJBO.FER_RF + TaskJBO.ODCOCR * ConstantJBO.CR_RF ;
				// 强制循环到底
				while(rs.next());
			}
		} catch (SQLException e) {
			throw new EngineSQLException("结果集获取市场风险标准法风险暴露临时表数据异常" , e);
		} finally {
			db.closeResultSet(rs);
			db.closeStatement(st);
		}
	}

}
