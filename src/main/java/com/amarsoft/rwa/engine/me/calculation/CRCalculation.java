package com.amarsoft.rwa.engine.me.calculation;

import com.amarsoft.rwa.engine.me.exception.EngineSQLException;
import com.amarsoft.rwa.engine.me.jbo.CommodityJBO;
import com.amarsoft.rwa.engine.me.jbo.ConstantJBO;
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
 * 商品风险计算
 * @author 陈庆
 * @version 1.0 2015-09-08
 *
 */
public class CRCalculation implements RWACalculation {

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
			
			// 汇总商品风险头寸结果
			List<CommodityJBO> commoditylist = this.getCommodityResult(db);
			
			// 计算商品风险资本要求
			commoditylist = this.calculateCommodityResult(commoditylist);
			
			// 插入商品风险结果
			this.insertCommodityResult(db, commoditylist);
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
				"WHERE MarketRiskType = '04' And OptionSimpleFlag = '0'";
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
				"WHERE MarketRiskType = '04' And OptionSimpleFlag = '0' AND DeltaPosition < 0";
		return db.executeUpdate(sql);
	}
	
	/**
	 * 获取商品风险头寸汇总结果
	 * @param db 数据库处理对象
	 * @return 返回商品风险头寸汇总结果列表
	 * @throws EngineSQLException 数据库操作异常
	 */
	private List<CommodityJBO> getCommodityResult(DBConnection db) throws EngineSQLException {
		List<CommodityJBO> list = new ArrayList<CommodityJBO>();
		CommodityJBO c = null;
		String sql = "SELECT CommodityName, SUM(CASE WHEN PositionType = '01' THEN DeltaPosition ELSE 0 END) AS GLP, " +
				"SUM(CASE WHEN PositionType = '02' THEN DeltaPosition ELSE 0 END) AS GSP " +
				"FROM RWA_ETC_MarketExposureSTD " +
				"WHERE MarketRiskType = '04' AND OptionSimpleFlag = '0' " +
				"GROUP BY CommodityName";
		Statement st = null;
		ResultSet rs = null;
		try {
			st = db.createStatement();
			rs = db.executeQuery(st, sql);
			while (rs.next()) {
				c = new CommodityJBO();
				c.setCommodityName(rs.getString("CommodityName"));
				c.setGlp(rs.getDouble("GLP"));
				c.setGsp(rs.getDouble("GSP"));
				list.add(c);
			}
		} catch (SQLException e) {
			throw new EngineSQLException("结果集获取市场风险标准法风险暴露临时表数据异常" , e);
		} finally {
			db.closeResultSet(rs);
			db.closeStatement(st);
		}
		return list;
	}
	
	/**
	 * 计算商品风险资本要求结果
	 * @param commoditylist 商品风险头寸汇总结果列表
	 * @return 返回商品风险资本要求结果列表
	 */
	private List<CommodityJBO> calculateCommodityResult(List<CommodityJBO> commoditylist) {
		for (CommodityJBO commodity : commoditylist) {
			commodity.setNp(commodity.getGlp() - commodity.getGsp());
			commodity.setGp(commodity.getGlp() + commodity.getGsp());
			commodity.setNppr(ConstantJBO.CRNPCRPR);
			commodity.setGppr(ConstantJBO.CRGPCRPR);
			commodity.setRc(commodity.getGp() * commodity.getGppr() + Math.abs(commodity.getNp()) * commodity.getNppr());
		}
		return commoditylist;
	}
	
	/**
	 * 按商品种类插入商品风险结果
	 * @param db 数据库处理对象
	 * @param commoditylist 商品风险结果列表
	 * @return 返回插入的记录数
	 * @throws EngineSQLException 数据库操作异常
	 * @throws ParseException 日期转换异常
	 */
	private int insertCommodityResult(DBConnection db, List<CommodityJBO> commoditylist) throws EngineSQLException, ParseException {
		String sql = "INSERT INTO RWA_ETR_Commodity" +
				"(CommodityName ,ResultSerialNo ,DataDate ,DataNo ,GLP ,GSP ,NP ,GP ,NPPR ,GPPR ,RC) " +
				"VALUES(?,?,?,?,?,?,?,?,?,?,?)";
		PreparedStatement pst = null;
		try {
			pst = db.prepareStatement(sql);
			for (CommodityJBO commodity : commoditylist) {
				this.insertCommodityResult(pst, commodity);
				// 汇总商品一般风险资本要求
				TaskJBO.CRGRCR = TaskJBO.CRGRCR + commodity.getRc();
			}
			db.executeBatch(pst);
		} finally {
			db.closePrepareStatement(pst);
		}
		return commoditylist.size();
	}
	
	/**
	 * 插入商品风险结果
	 * @param pst 插入商品风险结果的pst
	 * @param commodity 商品风险结果
	 * @throws EngineSQLException 数据库操作异常
	 * @throws ParseException 日期转换异常
	 */
	private void insertCommodityResult(PreparedStatement pst, CommodityJBO commodity) throws EngineSQLException, ParseException {
		try {
			pst.setString(1, commodity.getCommodityName());
			pst.setString(2, TaskJBO.resultNo);
			pst.setDate(3, EngineUtil.getFormatSqlDate(TaskJBO.dataDate));
			pst.setString(4, TaskJBO.dataNo);
			pst.setDouble(5, commodity.getGlp());
			pst.setDouble(6, commodity.getGsp());
			pst.setDouble(7, commodity.getNp());
			pst.setDouble(8, commodity.getGp());
			pst.setDouble(9, commodity.getNppr());
			pst.setDouble(10, commodity.getGppr());
			pst.setDouble(11, commodity.getRc());
			pst.addBatch();
		} catch (SQLException e) {
			throw new EngineSQLException("插入商品风险结果异常" , e);
		}
	}

}
