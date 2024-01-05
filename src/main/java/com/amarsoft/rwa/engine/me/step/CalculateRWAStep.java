package com.amarsoft.rwa.engine.me.step;

import com.amarsoft.rwa.engine.me.calculation.RWACalculationFactory;
import com.amarsoft.rwa.engine.me.calculation.RWACalculationType;
import com.amarsoft.rwa.engine.me.exception.EngineDataException;
import com.amarsoft.rwa.engine.me.exception.EngineParameterException;
import com.amarsoft.rwa.engine.me.exception.EngineSQLException;
import com.amarsoft.rwa.engine.me.jbo.ConstantJBO;
import com.amarsoft.rwa.engine.me.jbo.TaskJBO;
import com.amarsoft.rwa.engine.me.util.DBUtils;
import com.amarsoft.rwa.engine.me.util.EngineUtil;
import com.amarsoft.rwa.engine.me.util.db.DBConnection;
import com.amarsoft.rwa.engine.me.util.log.LogUtil;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.text.ParseException;
import java.util.Date;
import java.util.*;

/**
 * 步骤：RWA计算
 * <br>标准法计算利率风险、股票风险、外汇风险、商品风险、期权风险
 * 
 * @author 陈庆
 * @version 1.0 2013-06-05
 *
 */
@Slf4j
public class CalculateRWAStep implements Step {

	
	/**
	 * 构造一个具有给定步骤类型的<code>CalculationGREStep</code>对象。
	 * @param t 步骤类型
	 */
	public CalculateRWAStep(StepType t) {
		TaskJBO.currentStep = t;
		TaskJBO.currentStepTime = new Date();
	}
	
	/**
	 * 计算一般暴露的风险加权资产。
	 * @throws Exception 
	 */
	public void execute() throws Exception {
		log.info(TaskJBO.currentStep.getName() + "步骤开始");
		// 数据库连接
		DBConnection db = new DBConnection();
		try {
			// 创建连接
			db.createConnection();
			// 更新步骤日志
			LogUtil.updateStepLogByStart(db);
			
			// 目前只支持标准法
			if ("01".equals(TaskJBO.marketApproach)) {
				// 获取计算常量
				this.initConstant(db, TaskJBO.marketParamVerNo);
				log.info("初始化市场风险计算常量");
				
				// 计算常量校验
				this.checkParamsByConstant(db);
				log.info("市场风险计算常量校验");
				
				// 初始化期权计算方法
				this.initMOApproach(db, TaskJBO.schemeID);
				log.info("初始化期权计算方法");
				
				// 确定期权基础工具的特定风险计提比率
				this.updateSRPR(db);
				log.info("确定期权基础工具的特定风险计提比率");
				
				// 确定期权基础工具的一般风险计提比率
				this.updateGRPR(db);
				log.info("确定期权基础工具的一般风险计提比率");
				// 统计分析
				DBUtils.analyzeTable("RWA_ETC_MarketExposureSTD");
				// 利率风险
				RWACalculationFactory.getService(RWACalculationType.IRR).calculate();
				log.info("利率风险计算结束");
				
				// 股票风险
				RWACalculationFactory.getService(RWACalculationType.ER).calculate();
				log.info("股票风险计算结束");
				
				// 外汇风险
				RWACalculationFactory.getService(RWACalculationType.FER).calculate();
				log.info("外汇风险计算结束");
				
				// 商品风险
				RWACalculationFactory.getService(RWACalculationType.CR).calculate();
				log.info("商品风险计算结束");
				
				// 期权风险
				RWACalculationFactory.getService(RWACalculationType.OR).calculate();
				log.info("期权风险计算结束");
				
				// 统计结果
				this.statisticalMarketResult();
				log.info("统计市场风险资本要求结果");
				
				// 插入市场风险结果
				this.insertMarketResult(db);
				log.info("插入市场风险资本要求结果");
			}
			// 结果临时表统计分析
			// 利率风险时段结果临时表
			DBUtils.analyzeTable( "RWA_ETR_InterestRateTB");
			// 利率风险时区结果临时表
			DBUtils.analyzeTable( "RWA_ETR_InterestRateTZ");
			// 利率一般风险结果临时表
			DBUtils.analyzeTable( "RWA_ETR_InterestRateGR");
			// 股票风险分类结果临时表
			DBUtils.analyzeTable( "RWA_ETR_EquityType");
			// 股票风险结果临时表
			DBUtils.analyzeTable( "RWA_ETR_Equity");
			// 外汇风险分类结果临时表
			DBUtils.analyzeTable( "RWA_ETR_ExchangeType");
			// 外汇风险结果临时表
			DBUtils.analyzeTable( "RWA_ETR_Exchange");
			// 商品风险结果临时表
			DBUtils.analyzeTable( "RWA_ETR_Commodity");
			// 市场风险结果临时表
			DBUtils.analyzeTable( "RWA_ETR_Market");
			// 更新步骤日志
			LogUtil.updateStepLogByOver(db);
		} finally {
			// 关闭连接
			db.close();
		}
		log.info(TaskJBO.currentStep.getName() + "步骤结束");
	}
	
	/**
	 * 初始化市场风险计算常量
	 * @param db 数据库处理对象
	 * @param paramVerNo 参数版本流水号
	 * @throws EngineSQLException 数据库操作异常
	 * @throws EngineParameterException 参数异常
	 */
	public void initConstant(DBConnection db, String paramVerNo)
			throws EngineSQLException, EngineParameterException {
		String sql = "Select IRRMMCRPR, IRRDMCRPR, ERSRCRPR, ERGRCRPR, FERCRPR, CRNPCRPR, CRGPCRPR, IRR_RF, FER_RF, CR_RF, ER_RF "
				+ " From RWA_EP_MarketConstantSTD Where ParamVerNo = '" + paramVerNo + "'";
		Statement st = null;
		ResultSet rs = null;
		try {
			st = db.createStatement();
			rs = db.executeQuery(st, sql);
			if (rs.next()) {
				ConstantJBO.IRRMMCRPR = rs.getDouble("IRRMMCRPR");
				ConstantJBO.IRRDMCRPR = rs.getDouble("IRRDMCRPR");
				ConstantJBO.ERSRCRPR = rs.getDouble("ERSRCRPR");
				ConstantJBO.ERGRCRPR = rs.getDouble("ERGRCRPR");
				ConstantJBO.FERCRPR = rs.getDouble("FERCRPR");
				ConstantJBO.CRNPCRPR = rs.getDouble("CRNPCRPR");
				ConstantJBO.CRGPCRPR = rs.getDouble("CRGPCRPR");
				ConstantJBO.IRR_RF = rs.getDouble("IRR_RF");
				ConstantJBO.FER_RF = rs.getDouble("FER_RF");
				ConstantJBO.CR_RF = rs.getDouble("CR_RF");
				ConstantJBO.ER_RF = rs.getDouble("ER_RF");
				// 强制结果集走到底
				while(rs.next());
			} else {
				throw new EngineParameterException("市场风险标准法计算常量表参数错误,未设置计算常量,请检查！");
			}
		} catch (SQLException e) {
			throw new EngineSQLException("结果集获取市场风险标准法计算常量列表异常[参数版本流水号：" + paramVerNo + "]", e);
		} finally {
			db.closeResultSet(rs);
			db.closeStatement(st);
		}
	}
	
	/**
	 * 市场风险计算常量校验，若有0则，则抛出参数异常
	 * @param db 数据库操作对象
	 * @throws EngineParameterException 参数异常
	 */
	private void checkParamsByConstant(DBConnection db) throws EngineParameterException {
		if (ConstantJBO.IRRMMCRPR == 0) {
			throw new EngineParameterException("利率风险到期日法资本要求计提比率 为0");
		}
		if (ConstantJBO.IRRDMCRPR == 0) {
			throw new EngineParameterException("利率风险久期法资本要求计提比率 为0");
		}
		if (ConstantJBO.ERSRCRPR == 0) {
			throw new EngineParameterException("股票风险特定市场风险计提比率 为0");
		}
		if (ConstantJBO.ERGRCRPR == 0) {
			throw new EngineParameterException("股票风险一般市场风险计提比率 为0");
		}
		if (ConstantJBO.FERCRPR == 0) {
			throw new EngineParameterException("外汇风险资本要求计提比率 为0");
		}
		if (ConstantJBO.CRNPCRPR == 0) {
			throw new EngineParameterException("商品风险净头寸资本要求计提比率 为0");
		}
		if (ConstantJBO.CRGPCRPR == 0) {
			throw new EngineParameterException("商品风险总头寸资本要求计提比率 为0");
		}
		if (ConstantJBO.IRR_RF == 0) {
			throw new EngineParameterException("利率风险结果系数 为0");
		}
		if (ConstantJBO.FER_RF == 0) {
			throw new EngineParameterException("外汇风险结果系数 为0");
		}
		if (ConstantJBO.CR_RF == 0) {
			throw new EngineParameterException("商品风险结果系数 为0");
		}
		if (ConstantJBO.ER_RF == 0) {
			throw new EngineParameterException("股票风险结果系数 为0");
		}
	}
	
	/**
	 * 设定期权风险计算方法
	 * @param db 数据库处理对象
	 * @param schemeID 计算方案ID
	 * @throws EngineSQLException 数据库操作异常
	 * @throws EngineDataException 数据异常
	 */
	public void initMOApproach(DBConnection db, String schemeID) throws EngineSQLException, EngineDataException {
		// 获取计算方法期权风险计算方法设置列表
		List<Map<String, String>> list = getMOApproachMap(db, schemeID);
		// 确定期权风险计算方法
		this.updateMOApproach(db, list);
	}
	
	/**
	 * 获取计算方法期权风险计算方法设置列表
	 * @param db 数据库处理对象
	 * @param schemeID 计算方案ID
	 * @return 返回计算方法期权风险计算方法设置列表
	 * @throws EngineSQLException 数据库操作异常
	 */
	private List<Map<String, String>> getMOApproachMap(DBConnection db, String schemeID) throws EngineSQLException {
		List<Map<String, String>> list = new ArrayList<Map<String,String>>();
		Map<String, String> map = null;
		String sql = "Select SerialNo, OptionRiskType, MORApproach "
				+ "From RWA_EP_SchemeMOA Where SchemeID = '" + schemeID + "'";
		Statement st = null;
		ResultSet rs = null;
		try {
			st = db.createStatement();
			rs = db.executeQuery(st, sql);
			while (rs.next()) {
				map = new HashMap<String, String>();
				map.put("SerialNo", rs.getString("SerialNo"));
				map.put("OptionRiskType", rs.getString("OptionRiskType"));
				map.put("MORApproach", rs.getString("MORApproach"));
				list.add(map);
			}
		} catch (SQLException e) {
			throw new EngineSQLException("结果集获取计算方案市场风险方法设置表数据异常", e);
		} finally {
			db.closeResultSet(rs);
			db.closeStatement(st);
		}
		return list;
	}
	
	/**
	 * 确定期权风险计算方法
	 * @param db 数据库处理对象
	 * @param list 计算方法期权风险计算方法设置列表
	 * @throws EngineSQLException 数据库操作异常
	 * @throws EngineDataException 数据异常
	 */
	private void updateMOApproach(DBConnection db, List<Map<String, String>> list) throws EngineSQLException, EngineDataException {
		String sql = "";
		// 市场风险暴露 - 期权风险计算方法 确定
		for (Map<String, String> m : list) {
			sql = "Update RWA_ETC_MarketExposureSTD Set MORApproach = '" + m.get("MORApproach")
					+ "' Where OptionUnderlyingFlag = '1' AND OptionRiskType = '" + m.get("OptionRiskType") + "'";
			db.executeUpdate(sql);
		}
		// 有卖出期权的必须使用Delta+法计算
		sql = "Update RWA_ETC_MarketExposureSTD Set MORApproach = '010502' " +
				"WHERE MORApproach = '010501' AND OptionPositionType = '02'";
		db.executeUpdate(sql);
		// 同个期权风险类型下只能使用一个计算方法, Delta+法
		sql = "Update RWA_ETC_MarketExposureSTD Set MORApproach = '010502' " +
				"WHERE OptionRiskType IN (SELECT OptionRiskType FROM RWA_ETC_MarketExposureSTD " +
				" WHERE MORApproach IS NOT NULL GROUP BY OptionRiskType HAVING COUNT(DISTINCT MORApproach) > 1)";
		db.executeUpdate(sql);
		// 期权简易法标识
		sql = "Update RWA_ETC_MarketExposureSTD Set OptionSimpleFlag = (CASE WHEN MORApproach = '010501' THEN '1' ELSE '0' END)";
		db.executeUpdate(sql);
		// 采用delta+计算的期权风险 若存在必须字段(波动率 Volatility, 得尔塔值 Delta, 伽马值 Gamma, 维加值 Vega)为空，则抛出异常
		sql = "SELECT ExposureID " +
				"FROM RWA_ETC_MarketExposureSTD WHERE MORApproach = '010502' " +
				"AND (Volatility IS NULL OR Delta IS NULL OR Gamma IS NULL OR Vega IS NULL)";
		Statement st = null;
		ResultSet rs = null;
		try {
			st = db.createStatement();
			rs = db.executeQuery(st, sql);
			if (rs.next()) {
				// 强制循环到底
				while(rs.next());
				throw new EngineDataException("市场风险标准法风险暴露临时表 存在delta+法相关必要参数为空的数据");
			}
		} catch (SQLException e) {
			throw new EngineSQLException("结果集获取市场风险标准法风险暴露临时表数据异常", e);
		} finally {
			db.closeResultSet(rs);
			db.closeStatement(st);
		}
	}
	
	/**
	 * 确定期权基础工具特定风险计提比率
	 * @param db 数据库处理对象
	 * @return 更新记录数
	 * @throws EngineSQLException 数据库操作异常
	 */
	private int updateSRPR(DBConnection db) throws EngineSQLException {
		int r = 0;
		// 利率期权
		r = r + this.updateIROSRPR(db);
		// 股票期权
		r = r + this.updateEOSRPR(db);
		// 外汇期权
		r = r + this.updateFEOSRPR(db);
		// 商品期权
		r = r + this.updateCOSRPR(db);
		return r;
	}
	
	/**
	 * 确定利率期权特定风险计提比率
	 * @param db 数据库处理对象
	 * @return 更新记录数
	 * @throws EngineSQLException 数据库操作异常
	 */
	private int updateIROSRPR(DBConnection db) throws EngineSQLException {
		// 债券即为本身的特定风险计提比率，利率为0
		String sql = "UPDATE RWA_ETC_MarketExposureSTD SET SRPR = " +
				" (CASE WHEN InteRateRiskType = '02' THEN 0 ELSE SRPR END) " +
				" WHERE OptionUnderlyingFlag = '1' AND OptionRiskType = '01'";
		return db.executeUpdate(sql);
	}
	
	/**
	 * 确定股票期权特定风险计提比率
	 * @param db 数据库处理对象
	 * @return 更新记录数
	 * @throws EngineSQLException 数据库操作异常
	 */
	private int updateEOSRPR(DBConnection db) throws EngineSQLException {
		// 股票特定风险计提比率8%
		String sql = "UPDATE RWA_ETC_MarketExposureSTD SET SRPR = " + ConstantJBO.ERSRCRPR + 
				" WHERE OptionUnderlyingFlag = '1' AND OptionRiskType = '02'";
		return db.executeUpdate(sql);
	}
	
	/**
	 * 确定外汇期权特定风险计提比率
	 * @param db 数据库处理对象
	 * @return 更新记录数
	 * @throws EngineSQLException 数据库操作异常
	 */
	private int updateFEOSRPR(DBConnection db) throws EngineSQLException {
		// 外汇期权特定风险计提比率0
		String sql = "UPDATE RWA_ETC_MarketExposureSTD SET SRPR = 0 " + 
				" WHERE OptionUnderlyingFlag = '1' AND OptionRiskType = '03'";
		return db.executeUpdate(sql);
	}
	
	/**
	 * 确定商品期权特定风险计提比率
	 * @param db 数据库处理对象
	 * @return 更新记录数
	 * @throws EngineSQLException 数据库操作异常
	 */
	private int updateCOSRPR(DBConnection db) throws EngineSQLException {
		// 商品期权特定风险计提比率0
		String sql = "UPDATE RWA_ETC_MarketExposureSTD SET SRPR = 0 " + 
				" WHERE OptionUnderlyingFlag = '1' AND OptionRiskType = '04'";
		return db.executeUpdate(sql);
	}
	
	/**
	 * 确定期权基础工具一般风险计提比率
	 * @param db 数据库处理对象
	 * @return 更新记录数
	 * @throws EngineSQLException 数据库操作异常
	 */
	private int updateGRPR(DBConnection db) throws EngineSQLException {
		int r = 0;
		// 利率期权
		r = r + this.updateIROGRPR(db);
		// 股票期权
		r = r + this.updateEOGRPR(db);
		// 外汇期权
		r = r + this.updateFEOGRPR(db);
		// 商品期权
		r = r + this.updateCOGRPR(db);
		return r;
	}
	
	/**
	 * 确定利率期权一般风险计提比率
	 * @param db 数据库处理对象
	 * @return 更新记录数
	 * @throws EngineSQLException 数据库操作异常
	 */
	private int updateIROGRPR(DBConnection db) throws EngineSQLException {
		// 债券为 到期日法权重，利率为到期日法收益率变化
		String sql = "UPDATE RWA_ETC_MarketExposureSTD SET GRPR = " +
				" (CASE WHEN InteRateRiskType = '01' THEN MaturityRW ELSE MaturityYC END) " +
				" WHERE OptionUnderlyingFlag = '1' AND OptionRiskType = '01'";
		return db.executeUpdate(sql);
	}
	
	/**
	 * 确定股票期权一般风险计提比率
	 * @param db 数据库处理对象
	 * @return 更新记录数
	 * @throws EngineSQLException 数据库操作异常
	 */
	private int updateEOGRPR(DBConnection db) throws EngineSQLException {
		// 股票一般风险计提比率8%
		String sql = "UPDATE RWA_ETC_MarketExposureSTD SET GRPR = " + ConstantJBO.ERGRCRPR + 
				" WHERE OptionUnderlyingFlag = '1' AND OptionRiskType = '02'";
		return db.executeUpdate(sql);
	}
	
	/**
	 * 确定外汇期权一般风险计提比率
	 * @param db 数据库处理对象
	 * @return 更新记录数
	 * @throws EngineSQLException 数据库操作异常
	 */
	private int updateFEOGRPR(DBConnection db) throws EngineSQLException {
		// 外汇一般风险计提比率8%
		String sql = "UPDATE RWA_ETC_MarketExposureSTD SET GRPR = " + ConstantJBO.FERCRPR + 
				" WHERE OptionUnderlyingFlag = '1' AND OptionRiskType = '03'";
		return db.executeUpdate(sql);
	}
	
	/**
	 * 确定商品期权一般风险计提比率
	 * @param db 数据库处理对象
	 * @return 更新记录数
	 * @throws EngineSQLException 数据库操作异常
	 */
	private int updateCOGRPR(DBConnection db) throws EngineSQLException {
		// 商品一般风险计提比率15%
		String sql = "UPDATE RWA_ETC_MarketExposureSTD SET GRPR = " + ConstantJBO.CRNPCRPR + 
				" WHERE OptionUnderlyingFlag = '1' AND OptionRiskType = '04'";
		return db.executeUpdate(sql);
	}
	
	/**
	 * 统计市场风险资本要求结果
	 */
	private void statisticalMarketResult() {
		// 特定风险 = 利率特定风险 * 系数 + 股票特定风险 * 系数
		TaskJBO.SRCR = TaskJBO.IRRSRCR * ConstantJBO.IRR_RF + TaskJBO.ERSRCR * ConstantJBO.ER_RF;
		// 一般风险 = 5大特定风险 * 系数，期权在计算的时候已经考虑系数，此处不需要
		TaskJBO.GRCR = TaskJBO.IRRGRCR * ConstantJBO.IRR_RF + TaskJBO.ERGRCR * ConstantJBO.ER_RF
				+ TaskJBO.FERGRCR * ConstantJBO.FER_RF + TaskJBO.CRGRCR * ConstantJBO.CR_RF + TaskJBO.ORGRCR;
		// 市场风险 = 资产证券化特定风险 * 系数 + 特定风险 + 一般风险，证券化只有利率风险
		TaskJBO.RC = TaskJBO.ABSSRCR * ConstantJBO.IRR_RF + TaskJBO.SRCR + TaskJBO.GRCR;
		// 利率风险资本要求
		TaskJBO.IRR_CR = TaskJBO.IRRSRCR + TaskJBO.ABSSRCR + TaskJBO.IRRGRCR + TaskJBO.ODIROCR;
		// 外汇风险资本要求
		TaskJBO.FER_CR = TaskJBO.FERGRCR + TaskJBO.ODFEOCR;
		// 商品风险资本要求
		TaskJBO.CR_CR = TaskJBO.CRGRCR + TaskJBO.ODCOCR;
		// 股票风险资本要求
		TaskJBO.ER_CR = TaskJBO.ERSRCR + TaskJBO.ERGRCR + TaskJBO.ODEOCR;
		// RWA = RC * 12.5
		TaskJBO.RWA =  TaskJBO.RC * 12.5;
	}
	
	/**
	 * 插入市场风险资本要求结果
	 * @param db 数据库处理对象
	 * @return 返回插入的记录数(1)
	 * @throws EngineSQLException 数据库操作异常
	 * @throws ParseException 日期转换异常
	 */
	private int insertMarketResult(DBConnection db) throws EngineSQLException, ParseException {
		// 插入结果
		String sql = "INSERT INTO RWA_ETR_Market" +
				"(ResultSerialNo, DataDate, DataNo, TaskType, TaskID, ParamVerNo, SchemeID, " +
				"ConsolidateFlag, MRApproach, MIRRGRApproach, RWA, RC, GRCR, SRCR, " +
				"ABSSRCR, IRRSRCR, IRRGRCR, ERSRCR, ERGRCR, FERGRCR, CRGRCR, " +
				"ORGRCR, OSHSCR, OSLPCR, ODIROCR, ODEOCR, ODFEOCR, ODCOCR, IRR_CR, FER_CR, CR_CR, ER_CR) " +
				"VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		PreparedStatement pst = null;
		try {
			pst = db.prepareStatement(sql);
			pst.setString(1, TaskJBO.resultNo);
			pst.setDate(2, EngineUtil.getFormatSqlDate(TaskJBO.dataDate));
			pst.setString(3, TaskJBO.dataNo);
			pst.setString(4, TaskJBO.taskType);
			pst.setString(5, TaskJBO.schemeID);
			pst.setString(6, TaskJBO.marketParamVerNo);
			pst.setString(7, TaskJBO.schemeID);
			pst.setString(8, TaskJBO.consolidatedFlag);
			pst.setString(9, TaskJBO.marketApproach);
			pst.setString(10, TaskJBO.mirrgrApproach);
			pst.setDouble(11, TaskJBO.RWA);
			pst.setDouble(12, TaskJBO.RC);
			pst.setDouble(13, TaskJBO.GRCR);
			pst.setDouble(14, TaskJBO.SRCR);
			pst.setDouble(15, TaskJBO.ABSSRCR);
			pst.setDouble(16, TaskJBO.IRRSRCR);
			pst.setDouble(17, TaskJBO.IRRGRCR);
			pst.setDouble(18, TaskJBO.ERSRCR);
			pst.setDouble(19, TaskJBO.ERGRCR);
			pst.setDouble(20, TaskJBO.FERGRCR);
			pst.setDouble(21, TaskJBO.CRGRCR);
			pst.setDouble(22, TaskJBO.ORGRCR);
			pst.setNull(23, Types.DOUBLE); //巴三不需要
			pst.setNull(24, Types.DOUBLE); //巴三不需要
			pst.setDouble(25, TaskJBO.ODIROCR);
			pst.setDouble(26, TaskJBO.ODEOCR);
			pst.setDouble(27, TaskJBO.ODFEOCR);
			pst.setDouble(28, TaskJBO.ODCOCR);
			pst.setDouble(29, TaskJBO.IRR_CR);
			pst.setDouble(30, TaskJBO.FER_CR);
			pst.setDouble(31, TaskJBO.CR_CR);
			pst.setDouble(32, TaskJBO.ER_CR);
			pst.execute();
		} catch (SQLException e) {
			throw new EngineSQLException("插入市场风险结果异常" , e);
		} finally {
			db.closePrepareStatement(pst);
		}
		return 1;
	}

}
