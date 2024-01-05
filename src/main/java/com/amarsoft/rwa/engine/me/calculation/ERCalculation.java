package com.amarsoft.rwa.engine.me.calculation;

import com.amarsoft.rwa.engine.me.exception.EngineSQLException;
import com.amarsoft.rwa.engine.me.jbo.ConstantJBO;
import com.amarsoft.rwa.engine.me.jbo.EquityJBO;
import com.amarsoft.rwa.engine.me.jbo.TaskJBO;
import com.amarsoft.rwa.engine.me.util.EngineUtil;
import com.amarsoft.rwa.engine.me.util.db.DBConnection;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

/**
 * 股票风险计算
 * @author 陈庆
 * @version 1.0 2015-09-08
 *
 */
public class ERCalculation implements RWACalculation {

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
			
			// 股票风险分类结果, 先抵销, 再插入分类结果
			this.insertEquityTypeResult(db);
			
			// 获取股票风险结果
			List<EquityJBO> equityList = this.getEquityResult(db);
			
			// 计算股票风险结果
			this.calculateEquityResult(equityList);
			
			// 插入股票风险结果
			this.insertEquityResult(db, equityList);
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
				"WHERE MarketRiskType = '02' And OptionSimpleFlag = '0'";
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
				"WHERE MarketRiskType = '02' And OptionSimpleFlag = '0' AND DeltaPosition < 0";
		return db.executeUpdate(sql);
	}
	
	/**
	 * 插入股票风险分类结果
	 * @param db 数据库处理对象
	 * @return 更新记录数
	 * @throws EngineSQLException 数据库操作异常
	 */
	private int insertEquityTypeResult(DBConnection db) throws EngineSQLException {
		// OptionSimpleFlag 简易法无需计算
		// 同个市场中完全相同的股票或指数（交割月份相同）的多、空头匹配头寸可完全予以抵消
		// substr(DueDate, 0, 7) AS DeliMonth 交割月份
		// 标记: 股票期权的抵销问题
		String sql = "INSERT INTO RWA_ETR_EquityType" +
				"(ExchangeArea, EquityRiskType, ResultSerialNo, DataDate, DataNo, GLP, GSP) " +
				"SELECT A.ExchangeArea, A.EquityRiskType, '" + TaskJBO.resultNo +
				"' AS ResultSerialNo, TO_DATE('" + TaskJBO.dataDate +
				"','YYYY-MM-DD') AS DataDate, '" + TaskJBO.dataNo + "' AS DataNo, " +
				"SUM(CASE WHEN A.Position > 0 THEN A.Position ELSE 0 END) AS GLP, " +
				"SUM(CASE WHEN A.Position < 0 THEN -A.Position ELSE 0 END) AS GSP " +
				"FROM " +
				"(SELECT ExchangeArea, EquityRiskType, " +
				" (CASE WHEN PositionType = '01' THEN DeltaPosition ELSE -DeltaPosition END) AS Position " +
				" FROM RWA_ETC_MarketExposureSTD " +
				" WHERE MarketRiskType = '02' AND OptionSimpleFlag = '0') A " +
				"GROUP BY A.ExchangeArea, A.EquityRiskType";
		return db.executeUpdate(sql);
	}

	/**
	 * 按交易地区获取股票风险结果
	 * @param db 数据库处理对象
	 * @return 股票风险结果列表
	 * @throws EngineSQLException 数据库操作异常
	 */
	private List<EquityJBO> getEquityResult(DBConnection db) throws EngineSQLException {
		List<EquityJBO> list = new ArrayList<EquityJBO>();
		EquityJBO eq = null;
		String sql = "SELECT ExchangeArea, SUM(GLP) AS GLP, SUM(GSP) AS GSP " +
				"FROM RWA_ETR_EquityType GROUP BY ExchangeArea";
		Statement st = null;
		ResultSet rs = null;
		try {
			st = db.createStatement();
			rs = db.executeQuery(st, sql);
			while (rs.next()) {
				eq = new EquityJBO();
				eq.setExchangeArea(rs.getString("ExchangeArea"));
				eq.setGlp(rs.getDouble("GLP"));
				eq.setGsp(rs.getDouble("GSP"));
				list.add(eq);
			}
		} catch (SQLException e) {
			throw new EngineSQLException("结果集获取股票风险分类结果临时表数据异常" , e);
		} finally {
			db.closeResultSet(rs);
			db.closeStatement(st);
		}
		return list;
	}
	
	/**
	 * 计算股票风险结果
	 * @param equityList 股票风险结果列表
	 * @return 返回计算后的股票风险结果列表
	 */
	private List<EquityJBO> calculateEquityResult(List<EquityJBO> equityList) {
		for (EquityJBO eq : equityList) {
			// 计算特定风险、一般风险
			eq.setGp(eq.getGlp() + eq.getGsp());
			eq.setNp(eq.getGlp() - eq.getGsp());
			eq.setSrpr(ConstantJBO.ERSRCRPR);
			eq.setGrpr(ConstantJBO.ERGRCRPR);
			eq.setSrcr(eq.getGp() * eq.getSrpr());
			eq.setGrcr(Math.abs(eq.getNp()) * eq.getGrpr());
			eq.setRc(eq.getSrcr() + eq.getGrcr());
			// 汇总股票特定风险、股票一般风险
			TaskJBO.ERSRCR = TaskJBO.ERSRCR  + eq.getSrcr();
			TaskJBO.ERGRCR = TaskJBO.ERGRCR + eq.getGrcr();
		}
		return equityList;
	}
	
	/**
	 * 插入股票风险结果，并返回插入的记录数
	 * @param db 数据库处理对象
	 * @param equityList 股票风险结果列表
	 * @return 插入记录数
	 * @throws EngineSQLException 数据库操作异常
	 * @throws ParseException 日期转换异常
	 */
	private int insertEquityResult(DBConnection db, List<EquityJBO> equityList) throws EngineSQLException, ParseException {
		String sql = "INSERT INTO RWA_ETR_Equity" +
				"(ExchangeArea, ResultSerialNo, DataDate, DataNo, GLP, GSP, GP, NP, SRPR, GRPR, SRCR, GRCR, RC) " +
				"VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)";
		PreparedStatement pst = null;
		try {
			pst = db.prepareStatement(sql);
			for (EquityJBO equity : equityList) {
				this.insertEquityResult(pst, equity);
			}
			db.executeBatch(pst);
		} finally {
			db.closePrepareStatement(pst);
		}
		return equityList.size();
	}
	
	/**
	 * 插入股票风险结果
	 * @param pst 插入股票风险结果的pst
	 * @param equity 股票风险结果
	 * @throws EngineSQLException 数据库操作异常
	 * @throws ParseException 日期转换异常
	 */
	private void insertEquityResult(PreparedStatement pst, EquityJBO equity) throws EngineSQLException, ParseException {
		try {
			pst.setString(1, equity.getExchangeArea());
			pst.setString(2, TaskJBO.resultNo);
			pst.setDate(3, EngineUtil.getFormatSqlDate(TaskJBO.dataDate));
			pst.setString(4, TaskJBO.dataNo);
			pst.setDouble(5, equity.getGlp());
			pst.setDouble(6, equity.getGsp());
			pst.setDouble(7, equity.getGp());
			pst.setDouble(8, equity.getNp());
			pst.setDouble(9, equity.getSrpr());
			pst.setDouble(10, equity.getGrpr());
			pst.setDouble(11, equity.getSrcr());
			pst.setDouble(12, equity.getGrcr());
			pst.setDouble(13, equity.getRc());
			pst.addBatch();
		} catch (SQLException e) {
			throw new EngineSQLException("插入股票风险结果异常" , e);
		}
	}

}
