package com.amarsoft.rwa.engine.me.calculation;

import com.amarsoft.rwa.engine.me.exception.EngineSQLException;
import com.amarsoft.rwa.engine.me.jbo.ConstantJBO;
import com.amarsoft.rwa.engine.me.jbo.IRGeneralRisk;
import com.amarsoft.rwa.engine.me.jbo.TaskJBO;
import com.amarsoft.rwa.engine.me.jbo.TimeZoneJBO;
import com.amarsoft.rwa.engine.me.util.EngineUtil;
import com.amarsoft.rwa.engine.me.util.db.DBConnection;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 利率风险计算
 * @author 陈庆
 * @version 1.0 2015-09-08
 *
 */
public class IRRCalculation implements RWACalculation {

	/**
	 * 执行计算
	 */
	public void calculate() throws Exception {
		DBConnection db = new DBConnection();
		try {
			// 创建数据库连接
			db.createConnection();
			/*
			 * 计算头寸
			 */
			// 承销债券头寸 = 头寸 * 转换系数（承销债券必定非期权）
			this.updateUnderBondPosition(db);
			
			// 得尔塔加权头寸 
			// 期权基础工具，得尔塔法 : 头寸 * 得尔塔值 
			// 其他： 头寸
			this.updateDeltaPosition(db);
			
			// 头寸调整
			// delta可能为负，则相应的需要转换
			this.adjustDeltaPosition(db);
			
			/*
			 * 计算利率特定风险(只有债券)
			 */
			// 计算特定市场风险资本要求
			this.updateSRCR(db);
			
			// 统计特定市场风险资本要求结果
			this.statisticalSRCRResult(db);
			/*
			 * 计算利率一般风险
			 */
			// 计算时段垂直资本要求
			if ("010101".equals(TaskJBO.mirrgrApproach)) {
				// 到期日法计算
				this.insertTimeBandResultByMaturity(db);
				// 计算垂直资本要求
				this.updateTimeBandVCR(db, ConstantJBO.IRRMMCRPR);
			} else {
				// 计算价格敏感性
				this.updatePriceSensitivity(db);
				// 久期法计算
				this.insertTimeBandResultByDuration(db);
				// 计算垂直资本要求
				this.updateTimeBandVCR(db, ConstantJBO.IRRDMCRPR);
			}
			
			// 获取时区权重
			Map<String, Double> tzrwMap = this.getTimeZoneRWMap(db, TaskJBO.marketParamVerNo);
			
			// 获取时区结果
			Map<String, List<TimeZoneJBO>> tzMap = this.getTimeZoneResult(db);
			
			// 计算各个币种的一般风险结果
			List<IRGeneralRisk> irgrList = this.calculateTimeZoneGRCR(tzMap, tzrwMap);
			
			// 插入各币种的时区结果
			this.insertTimeZoneResult(db, tzMap);
			// 插入各币种的一般风险结果
			this.insertGeneralRiskResult(db, irgrList);
		} finally {
			db.close();
		}
	}
	
	/**
	 * 计算承销债券头寸
	 * @param db 数据库处理对象
	 * @return 更新记录数
	 * @throws EngineSQLException 数据库操作异常
	 */
	private int updateUnderBondPosition(DBConnection db) throws EngineSQLException {
		String sql = "UPDATE RWA_ETC_MarketExposureSTD SET UnderBondPosition = Position * CF " +
				"WHERE MarketRiskType = '01' And UnderBondFlag = '1' ";
		return db.executeUpdate(sql);
	}
	
	/**
	 * 计算得尔塔加权头寸
	 * @param db 数据库处理对象
	 * @return 更新记录数
	 * @throws EngineSQLException 数据库操作异常
	 */
	private int updateDeltaPosition(DBConnection db) throws EngineSQLException {
		// OptionSimpleFlag = '0' 简易法无需在这边计算RWA
		String sql = "UPDATE RWA_ETC_MarketExposureSTD SET DeltaPosition = " +
				"(CASE WHEN UnderBondFlag = '1' THEN UnderBondPosition " +
				"WHEN OptionUnderlyingFlag = '1' AND MORApproach = '010502' THEN Position * Delta " +
				"ELSE Position END) " +
				"WHERE MarketRiskType = '01' And OptionSimpleFlag = '0'";
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
				"WHERE MarketRiskType = '01' And OptionSimpleFlag = '0' AND DeltaPosition < 0";
		return db.executeUpdate(sql);
	}
	
	/**
	 * 计算特定市场风险资本要求
	 * @param db 数据库处理对象
	 * @return 更新记录数
	 * @throws EngineSQLException 数据库操作异常
	 */
	private int updateSRCR(DBConnection db) throws EngineSQLException {
		// OptionSimpleFlag = '0' 简易法无需在利率风险计算RWA
		String sql = "UPDATE RWA_ETC_MarketExposureSTD SET SRCR = DeltaPosition * SRPR " +
				"WHERE MarketRiskType = '01' AND InteRateRiskType = '01' And OptionSimpleFlag = '0'";
		return db.executeUpdate(sql);
	}
	
	/**
	 * 统计利率特定风险结果
	 * @param db 数据库处理对象
	 * @throws EngineSQLException 数据库操作异常
	 */
	private void statisticalSRCRResult(DBConnection db) throws EngineSQLException {
		// OptionSimpleFlag = '0' 简易法无需在利率风险计算RWA
		String sql = "SELECT SUM(CASE WHEN SecuritiesType = '03' THEN SRCR ELSE 0 END) AS ABSSRCR, " +
				"SUM(CASE WHEN SecuritiesType = '03' THEN 0 ELSE SRCR END) AS IRRSRCR " +
				"FROM RWA_ETC_MarketExposureSTD " +
				"WHERE MarketRiskType = '01' AND InteRateRiskType = '01' And OptionSimpleFlag = '0'";
		Statement st = null;
		ResultSet rs = null;
		try {
			st = db.createStatement();
			rs = db.executeQuery(st, sql);
			if (rs.next()) {
				TaskJBO.ABSSRCR = rs.getDouble("ABSSRCR");
				TaskJBO.IRRSRCR = rs.getDouble("IRRSRCR");
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
	
	/**
	 * 计算价格敏感性
	 * @param db 数据库处理对象
	 * @return 影响记录数
	 * @throws EngineSQLException 数据库操作异常
	 */
	private int updatePriceSensitivity(DBConnection db) throws EngineSQLException {
		// 简易法无需在利率风险计算RWA
		String sql = "UPDATE RWA_ETC_MarketExposureSTD SET PriceSensitivity = DeltaPosition * ModifiedDuration" +
				"WHERE MarketRiskType = '01' And OptionSimpleFlag = '0'";
		return db.executeUpdate(sql);
	}
	
	/**
	 * 插入到期日法时段结果
	 * @param db 数据库处理对象
	 * @return 影响记录数
	 * @throws EngineSQLException 数据库操作异常
	 */
	private int insertTimeBandResultByMaturity(DBConnection db) throws EngineSQLException {
		String sql = "INSERT INTO RWA_ETR_InterestRateTB" +
				"(Currency, TimeBandType, ResultSerialNo, DataDate, DataNo, TimeZone, RW, " +
				"BondLP, BondSP, RateLP, RateSP, GLP, GSP, WLP, WSP) " +
				"SELECT Currency, MaturityTimeBandType AS TimeBandType, '" + TaskJBO.resultNo + 
				"' AS ResultSerialNo, TO_DATE('" + TaskJBO.dataDate + 
				"','YYYY-MM-DD') AS DataDate, '" + TaskJBO.dataNo + 
				"' AS DataNo, MaturityTimeZone AS TimeZone, MaturityRW AS RW, " +
				"SUM(CASE WHEN InteRateRiskType = '01' AND PositionType = '01' THEN DeltaPosition ELSE 0 END) AS BondLP, " +
				"SUM(CASE WHEN InteRateRiskType = '01' AND PositionType = '02' THEN DeltaPosition ELSE 0 END) AS BondSP, " +
				"SUM(CASE WHEN InteRateRiskType = '02' AND PositionType = '01' THEN DeltaPosition ELSE 0 END) AS RateLP, " +
				"SUM(CASE WHEN InteRateRiskType = '02' AND PositionType = '02' THEN DeltaPosition ELSE 0 END) AS RateSP, " +
				"SUM(CASE WHEN PositionType = '01' THEN DeltaPosition ELSE 0 END) AS GLP, " +
				"SUM(CASE WHEN PositionType = '02' THEN DeltaPosition ELSE 0 END) AS GSP, " +
				"SUM(CASE WHEN PositionType = '01' THEN DeltaPosition ELSE 0 END) * MaturityRW AS WLP, " +
				"SUM(CASE WHEN PositionType = '02' THEN DeltaPosition ELSE 0 END) * MaturityRW AS WSP " +
				"FROM RWA_ETC_MarketExposureSTD " +
				"WHERE MarketRiskType = '01' AND OptionSimpleFlag = '0' " +
				"GROUP BY Currency, MaturityTimeBandType, MaturityTimeZone, MaturityRW";
		return db.executeUpdate(sql);
	}
	
	/**
	 * 插入久期法时段结果
	 * @param db 数据库处理对象
	 * @return 影响记录数
	 * @throws EngineSQLException 数据库操作异常
	 */
	private int insertTimeBandResultByDuration(DBConnection db) throws EngineSQLException {
		String sql = "INSERT INTO RWA_ETR_InterestRateTB" +
				"(Currency, TimeBandType, ResultSerialNo, DataDate, DataNo, TimeZone, RW, " +
				"BondLP, BondSP, RateLP, RateSP, GLP, GSP, WLP, WSP) " +
				"SELECT Currency, DurationTimeBandType AS TimeBandType, '" + TaskJBO.resultNo + 
				"' AS ResultSerialNo, TO_DATE('" + TaskJBO.dataDate + 
				"','YYYY-MM-DD') AS DataDate, '" + TaskJBO.dataNo + 
				"' AS DataNo, DurationTimeZone AS TimeZone, DurationYC AS RW, " +
				"SUM(CASE WHEN InteRateRiskType = '01' AND PositionType = '01' THEN PriceSensitivity ELSE 0 END) AS BondLP, " +
				"SUM(CASE WHEN InteRateRiskType = '01' AND PositionType = '02' THEN PriceSensitivity ELSE 0 END) AS BondSP, " +
				"SUM(CASE WHEN InteRateRiskType = '02' AND PositionType = '01' THEN PriceSensitivity ELSE 0 END) AS RateLP, " +
				"SUM(CASE WHEN InteRateRiskType = '02' AND PositionType = '02' THEN PriceSensitivity ELSE 0 END) AS RateSP, " +
				"SUM(CASE WHEN PositionType = '01' THEN PriceSensitivity ELSE 0 END) AS GLP, " +
				"SUM(CASE WHEN PositionType = '02' THEN PriceSensitivity ELSE 0 END) AS GSP, " +
				"SUM(CASE WHEN PositionType = '01' THEN PriceSensitivity ELSE 0 END) * DurationYC AS WLP, " +
				"SUM(CASE WHEN PositionType = '02' THEN PriceSensitivity ELSE 0 END) * DurationYC AS WSP " +
				"FROM RWA_ETC_MarketExposureSTD " +
				"WHERE MarketRiskType = '01' AND OptionSimpleFlag = '0' " +
				"GROUP BY Currency, DurationTimeBandType, DurationTimeZone, DurationYC";
		return db.executeUpdate(sql);
	}
	
	/**
	 * 计算时段垂直资本要求
	 * @param db 数据库处理对象
	 * @param pr 计提比率
	 * @return 影响记录数
	 * @throws EngineSQLException 数据库操作异常
	 */
	private int updateTimeBandVCR(DBConnection db, double pr) throws EngineSQLException {
		String sql = "UPDATE RWA_ETR_InterestRateTB SET " +
				" WHP = (CASE WHEN WLP > WSP THEN WSP ELSE WLP END), WNP = WLP - WSP, " +
				" PR = " + pr + 
				", VCR = (CASE WHEN WLP > WSP THEN WSP ELSE WLP END) * " + pr;
		return db.executeUpdate(sql);
	}
	
	/**
	 * 获取时区权重集合
	 * <br>String 为时区1+时区2，double即为对应权重
	 * @param db 数据库处理对象
	 * @param paramVerNo 参数版本流水号
	 * @return 返回时区权重集合
	 * @throws EngineSQLException 数据库操作异常
	 */
	private Map<String, Double> getTimeZoneRWMap(DBConnection db, String paramVerNo) throws EngineSQLException {
		Map<String, Double> map = new HashMap<String, Double>();
		String sql = "SELECT TimeZone1, TimeZone2, RW " +
				"FROM RWA_EP_TimeZoneRW WHERE ParamVerNo = '" + paramVerNo + "'";
		Statement st = null;
		ResultSet rs = null;
		try {
			st = db.createStatement();
			rs = db.executeQuery(st, sql);
			while (rs.next()) {
				map.put(rs.getString("TimeZone1") + rs.getString("TimeZone2"), rs.getDouble("RW"));
				map.put(rs.getString("TimeZone2") + rs.getString("TimeZone1"), rs.getDouble("RW"));
			}
		} catch (SQLException e) {
			throw new EngineSQLException("结果集获取时区风险权重表数据异常" , e);
		} finally {
			db.closeResultSet(rs);
			db.closeStatement(st);
		}
		return map;
	}
	
	/**
	 * 获取各币种各时区的结果集合
	 * <br>String为币种，list即为币种的时区列表
	 * @param db 数据库处理对象
	 * @return 返回各币种各时区结果
	 * @throws EngineSQLException 数据库操作异常
	 */
	private Map<String, List<TimeZoneJBO>> getTimeZoneResult(DBConnection db) throws EngineSQLException {
		Map<String, List<TimeZoneJBO>> map = new HashMap<String, List<TimeZoneJBO>>();
		List<TimeZoneJBO> list = null;
		TimeZoneJBO tz = null;
		String sql = "SELECT Currency, TimeZone, SUM(VCR) AS VCR, " +
				"SUM(CASE WHEN WNP > 0 THEN WNP ELSE 0 END) AS WLP, " +
				"SUM(CASE WHEN WNP < 0 THEN -WNP ELSE 0 END) AS WSP, " +
				"SUM(WNP) AS WNP " +
				"FROM RWA_ETR_InterestRateTB " +
				"GROUP BY Currency, TimeZone ORDER BY Currency, TimeZone";
		Statement st = null;
		ResultSet rs = null;
		try {
			st = db.createStatement();
			rs = db.executeQuery(st, sql);
			while (rs.next()) {
				tz = new TimeZoneJBO();
				tz.setCurrency(rs.getString("Currency"));
				tz.setTimeZone1(rs.getString("TimeZone"));
				tz.setTimeZone2(rs.getString("TimeZone"));
				tz.setVcr(rs.getDouble("VCR"));
				tz.setWlp(rs.getDouble("WLP"));
				tz.setWsp(rs.getDouble("WSP"));
				tz.setWhp(Math.min(tz.getWlp(), tz.getWsp()));
				tz.setWnp(rs.getDouble("WNP"));
				if (map.get(tz.getCurrency()) == null) {
					list = new ArrayList<TimeZoneJBO>();
					list.add(tz);
					map.put(tz.getCurrency(), list);
				} else {
					map.get(tz.getCurrency()).add(tz);
				}
			}
		} catch (SQLException e) {
			throw new EngineSQLException("结果集获取利率风险时段结果临时表数据异常" , e);
		} finally {
			db.closeResultSet(rs);
			db.closeStatement(st);
		}
		return map;
	}
	
	/**
	 * 计算第一区与第二区之间的对冲头寸
	 * @param np1 第一区加权净头寸
	 * @param np2 第二区加权净头寸
	 * @return 返回第一区与第二区之间的对冲头寸
	 */
	private double calculateTimeZone12WHP(double np1, double np2) {
		double hp = 0;
		if ((np1 > 0 && np2 < 0) || (np1 <0 && np2 > 0)) {
			hp = Math.min(Math.abs(np1), Math.abs(np2));
		}
		return hp;
	}
	
	/**
	 * 计算第二区与第三区之间的对冲头寸
	 * @param np1 第一区加权净头寸
	 * @param np2 第二区加权净头寸
	 * @param np3 第三区加权净头寸
	 * @param hp12 第一区与第二区之间的对冲头寸
	 * @return 返回第二区与第三区之间的对冲头寸
	 */
	private double calculateTimeZone23WHP(double np1, double np2, double np3, double hp12) {
		double hp = 0;
		if ((np2 > 0 && np3 < 0) || (np2 <0 && np3 > 0)) {
			if (hp12 == 0) {
				hp = Math.min(Math.abs(np2), Math.abs(np3));
			} else {
				if (Math.abs(np1) < Math.abs(np2)) {
					hp = Math.min(Math.abs(np2 + np1), Math.abs(np3));
				}
			}
		}
		return hp;
	}
	
	/**
	 * 计算第一区与第三区之间的对冲头寸
	 * @param np1 第一区加权净头寸
	 * @param np2 第二区加权净头寸
	 * @param np3 第三区加权净头寸
	 * @param hp12 第一区与第二区之间的对冲头寸
	 * @param hp23 第二区与第三区之间的对冲头寸
	 * @return 返回第一区与第三区之间的对冲头寸
	 */
	private double calculateTimeZone13WHP(double np1, double np2, double np3, double hp12, double hp23) {
		double hp = 0;
		double s1 = Math.signum(np1);
		double s2 = Math.signum(np2);;
		double s3 = Math.signum(np3);;
		if (s1 == s2 && s2 == s3) {
			hp = 0;
		} else {
			if (s1 == s2) {
				hp23 = Math.min(Math.abs(np2), Math.abs(np3));
			} else {
				if (Math.signum((np1 + np2)) != s3 && s2 != s3) {
					hp23 = Math.min(Math.abs(np2 + np1), Math.abs(np3));
				} 
			}
		}
		hp = -(Math.abs(np1 + np2 + np3) - Math.abs(np1) - Math.abs(np2) - Math.abs(np3)) / 2 - hp12 - hp23;
		return hp;
	}
	
	/**
	 * 计算各个币种的一般风险结果
	 * @param tzMap 各币种各时区结果集合
	 * @param tzrwMap 时区权重
	 * @return 返回一般风险结果
	 */
	private List<IRGeneralRisk> calculateTimeZoneGRCR(Map<String, List<TimeZoneJBO>> tzMap, Map<String, Double> tzrwMap) {
		List<IRGeneralRisk> list = new ArrayList<IRGeneralRisk>();
		for (String s : tzMap.keySet()) {
			list.add(this.calculateTimeZoneGRCR(s, tzMap.get(s), tzrwMap));
		}
		return list;
	}
	
	/**
	 * 计算该币种的一般风险结果
	 * @param currency 币种
	 * @param tzList 各时区结果列表
	 * @param tzrwMap 时区权重集合
	 * @return 返回该币种的一般风险结果
	 */
	private IRGeneralRisk calculateTimeZoneGRCR(String currency, List<TimeZoneJBO> tzList, Map<String, Double> tzrwMap) {
		// 各币种的利率一般风险结果
		IRGeneralRisk irgr = new IRGeneralRisk();
		irgr.setCurrency(currency);
		double np1 = 0;
		double np2 = 0;
		double np3 = 0;
		double vcr = 0;
		double wnp = 0;
		/*
		 * 计算每个时区的横向资本要求，并汇总相关结果
		 */
		for (TimeZoneJBO tz : tzList) {
			// 计算时区内的横向资本要求
			tz.setRw(tzrwMap.get(tz.getTimeZone1() + tz.getTimeZone2()));
			tz.setHcr(tz.getWhp() * tz.getRw());
			// 第一区净头寸、横向资本要求
			if ("01".equals(tz.getTimeZone1())) {
				np1 = tz.getWnp();
				irgr.setHcr11(tz.getHcr());
			}
			// 第二区净头寸、横向资本要求
			if ("02".equals(tz.getTimeZone1())) {
				np2 = tz.getWnp();
				irgr.setHcr22(tz.getHcr());
			}
			// 第三区净头寸、横向资本要求
			if ("03".equals(tz.getTimeZone1())) {
				np3 = tz.getWnp();
				irgr.setHcr33(tz.getHcr());
			}
			// 汇总垂直资本要求
			vcr = vcr + tz.getVcr();
			// 汇总加权净头寸
			wnp = wnp + tz.getWnp();
		}
		// 垂直资本要求
		irgr.setGvcr(vcr);
		// 交易账户加权净头寸
		irgr.setWnp(wnp);
		// 交易账户资本要求
		irgr.setTbcr(Math.abs(irgr.getWnp()));
		/*
		 * 计算时区间的横向资本要求
		 */
		// 计算时区间的对冲头寸
		double hp12 = this.calculateTimeZone12WHP(np1, np2);
		double hp23 = this.calculateTimeZone23WHP(np1, np2, np3, hp12);
		double hp13 = this.calculateTimeZone13WHP(np1, np2, np3, hp12, hp23);
		TimeZoneJBO tz = null;
		// 保存1区与2区间的对冲头寸、横向资本要求结果
		tz = new TimeZoneJBO();
		tz.setCurrency(currency);
		tz.setTimeZone1("01");
		tz.setTimeZone2("02");
		tz.setWhp(hp12);
		tz.setRw(tzrwMap.get(tz.getTimeZone1() + tz.getTimeZone2()));
		tz.setHcr(tz.getWhp() * tz.getRw());
		tzList.add(tz);
		irgr.setHcr12(tz.getHcr());
		// 保存2区与3区间的对冲头寸、横向资本要求结果
		tz = new TimeZoneJBO();
		tz.setCurrency(currency);
		tz.setTimeZone1("02");
		tz.setTimeZone2("03");
		tz.setWhp(hp23);
		tz.setRw(tzrwMap.get(tz.getTimeZone1() + tz.getTimeZone2()));
		tz.setHcr(tz.getWhp() * tz.getRw());
		tzList.add(tz);
		irgr.setHcr23(tz.getHcr());
		// 保存1区与3区间的对冲头寸、横向资本要求结果
		tz = new TimeZoneJBO();
		tz.setCurrency(currency);
		tz.setTimeZone1("01");
		tz.setTimeZone2("03");
		tz.setWhp(hp13);
		tz.setRw(tzrwMap.get(tz.getTimeZone1() + tz.getTimeZone2()));
		tz.setHcr(tz.getWhp() * tz.getRw());
		tzList.add(tz);
		irgr.setHcr13(tz.getHcr());
		// 汇总横向资本要求
		irgr.setGhcr(irgr.getHcr11() + irgr.getHcr22() + irgr.getHcr33() + irgr.getHcr12() + irgr.getHcr23() + irgr.getHcr13());
		// 汇总一般风险资本要求
		irgr.setGrcr(irgr.getGvcr() + irgr.getGhcr() + irgr.getTbcr());
		// 利率一般风险资本要求汇总
		TaskJBO.IRRGRCR = TaskJBO.IRRGRCR + irgr.getGrcr();
		return irgr;
	}
	
	/**
	 * 插入各币种的时区结果
	 * @param db 数据库处理对象
	 * @param tzMap 时区结果集合
	 * @return 返回插入的记录数
	 * @throws EngineSQLException 数据库操作异常
	 * @throws ParseException 日期转换异常
	 */
	private int insertTimeZoneResult(DBConnection db, Map<String, List<TimeZoneJBO>> tzMap) throws EngineSQLException, ParseException {
		int r = 0;
		String sql = "INSERT INTO RWA_ETR_InterestRateTZ" +
				"(Currency, TimeZone1, TimeZone2, ResultSerialNo, DataDate, DataNo, " +
				"WLP, WSP, WHP, WNP, RW, HCR) " +
				"VALUES(?,?,?,?,?,?,?,?,?,?,?,?)";
		PreparedStatement pst = null;
		try {
			pst = db.prepareStatement(sql);
			for (List<TimeZoneJBO> list : tzMap.values()) {
				for (TimeZoneJBO tz : list) {
					this.insertTimeZoneResult(pst, tz);
				}
				r = r + list.size();
			}
			db.executeBatch(pst);
		} finally {
			db.closePrepareStatement(pst);
		}
		return r;
	}
	
	/**
	 * 插入时区结果
	 * @param pst 插入时区结果的pst
	 * @param tz 时区结果
	 * @throws EngineSQLException 数据库操作异常
	 * @throws ParseException 日期转换系数
	 */
	private void insertTimeZoneResult(PreparedStatement pst, TimeZoneJBO tz) throws EngineSQLException, ParseException {
		try {
			pst.setString(1, tz.getCurrency());
			pst.setString(2, tz.getTimeZone1());
			pst.setString(3, tz.getTimeZone2());
			pst.setString(4, TaskJBO.resultNo);
			pst.setDate(5, EngineUtil.getFormatSqlDate(TaskJBO.dataDate));
			pst.setString(6, TaskJBO.dataNo);
			pst.setDouble(7, tz.getWlp());
			pst.setDouble(8, tz.getWsp());
			pst.setDouble(9, tz.getWhp());
			pst.setDouble(10, tz.getWnp());
			pst.setDouble(11, tz.getRw());
			pst.setDouble(12, tz.getHcr());
			pst.addBatch();
		} catch (SQLException e) {
			throw new EngineSQLException("插入利率风险时区结果异常", e);
		}
	}
	
	/**
	 * 插入各币种的一般风险结果
	 * @param db 数据库处理对象
	 * @param irgrList 一般风险结果列表
	 * @return 插入的记录数
	 * @throws EngineSQLException 数据库操作异常
	 * @throws ParseException 日期转换异常
	 */
	private int insertGeneralRiskResult(DBConnection db, List<IRGeneralRisk> irgrList) throws EngineSQLException, ParseException {
		String sql = "INSERT INTO RWA_ETR_InterestRateGR" +
				"(Currency, ResultSerialNo, DataDate, DataNo, MIRRGRApproach, GVCR, " +
				"Zone11CR, Zone22CR, Zone33CR, Zone12CR, Zone23CR, Zone13CR, GHCR, TBWNP, TBCR, GRCR) " +
				"VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		PreparedStatement pst = null;
		try {
			pst = db.prepareStatement(sql);
			for (IRGeneralRisk irgr : irgrList) {
				this.insertGeneralRiskResult(pst, irgr);
			}
			db.executeBatch(pst);
		} finally {
			db.closePrepareStatement(pst);
		}
		return irgrList.size();
	}
	
	/**
	 * 插入一般风险结果
	 * @param pst 插入利率一般风险结果的pst
	 * @param irgr 一般风险结果
	 * @throws EngineSQLException 数据库操作异常
	 * @throws ParseException 日期转换系数
	 */
	private void insertGeneralRiskResult(PreparedStatement pst, IRGeneralRisk irgr) throws EngineSQLException, ParseException {
		try {
			pst.setString(1, irgr.getCurrency());
			pst.setString(2, TaskJBO.resultNo);
			pst.setDate(3, EngineUtil.getFormatSqlDate(TaskJBO.dataDate));
			pst.setString(4, TaskJBO.dataNo);
			pst.setString(5, TaskJBO.mirrgrApproach);
			pst.setDouble(6, irgr.getGvcr());
			pst.setDouble(7, irgr.getHcr11());
			pst.setDouble(8, irgr.getHcr22());
			pst.setDouble(9, irgr.getHcr33());
			pst.setDouble(10, irgr.getHcr12());
			pst.setDouble(11, irgr.getHcr23());
			pst.setDouble(12, irgr.getHcr13());
			pst.setDouble(13, irgr.getGhcr());
			pst.setDouble(14, irgr.getWnp());
			pst.setDouble(15, irgr.getTbcr());
			pst.setDouble(16, irgr.getGrcr());
			pst.addBatch();
		} catch (SQLException e) {
			throw new EngineSQLException("插入利率一般风险结果异常", e);
		}
	}

}
