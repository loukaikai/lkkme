package com.amarsoft.rwa.engine.me.step;

import com.amarsoft.rwa.engine.me.exception.EngineSQLException;
import com.amarsoft.rwa.engine.me.jbo.TaskJBO;
import com.amarsoft.rwa.engine.me.util.DBUtils;
import com.amarsoft.rwa.engine.me.util.db.DBConnection;
import com.amarsoft.rwa.engine.me.util.log.LogUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.Date;

/**
 * 步骤：结果写入
 * <br>将RWA结果从临时表写到结果表
 * @author 陈庆
 * @version 1.0 2013-06-05
 */
@Slf4j
public class InsertResultStep implements Step {
	
	/**
	 * 构造一个具有给定步骤类型的<code>InsertResultStep</code>对象。
	 * @param t 步骤类型
	 */
	public InsertResultStep(StepType t) {
		TaskJBO.currentStep = t;
		TaskJBO.currentStepTime = new Date();
	}
	
	/**
	 * 将RWA结果从结果临时表写到结果表
	 * @throws Exception
	 */
	public void execute() throws Exception {
		log.info(TaskJBO.currentStep.getName() + "步骤开始");
		
		// 数据库连接
		DBConnection db = new DBConnection();
		try {
			// 创建连接
			db.createConnection();
			
			// 更新步骤开始日志
			LogUtil.updateStepLogByStart(db);
			
			int r = 0;
			r = insertMarketExposure(db);
			log.info("插入市场风险标准法风险暴露结果表(" + r + ")");
			
			r = insertInterestRateTB(db);
			log.info("插入利率风险时段结果表(" + r + ")");
			
			r = insertInterestRateTZ(db);
			log.info("插入利率风险时区结果表(" + r + ")");
			
			r = insertInterestRateGR(db);
			log.info("插入利率一般风险结果表(" + r + ")");
			
			r = insertEquityType(db);
			log.info("插入股票风险分类结果表(" + r + ")");
			
			r = insertEquity(db);
			log.info("插入股票风险结果表(" + r + ")");
			
			r = insertExchangeType(db);
			log.info("插入外汇风险分类结果表(" + r + ")");
			
			r = insertExchange(db);
			log.info("插入外汇风险结果表(" + r + ")");
			
			r = insertCommodity(db);
			log.info("插入商品风险结果表(" + r + ")");
			
			r = insertMarket(db);
			log.info("插入市场风险结果表(" + r + ")");

			// 结果表统计分析
			DBUtils.analyzeTable("RWA_ERR_MarketExposureSTD");
			DBUtils.analyzeTable("RWA_ERR_InterestRateTB");
			DBUtils.analyzeTable("RWA_ERR_InterestRateTZ");
			DBUtils.analyzeTable("RWA_ERR_InterestRateGR");
			DBUtils.analyzeTable("RWA_ERR_EquityType");
			DBUtils.analyzeTable("RWA_ERR_Equity");
			DBUtils.analyzeTable("RWA_ERR_ExchangeType");
			DBUtils.analyzeTable("RWA_ERR_Exchange");
			DBUtils.analyzeTable("RWA_ERR_Commodity");
			DBUtils.analyzeTable("RWA_ERR_Market");

			// 更新步骤正常结束日志
			LogUtil.updateStepLogByOver(db);
		} finally {
			// 关闭连接
			db.close();
		}
		log.info(TaskJBO.currentStep.getName() + "步骤结束");
	}
	
	/**
	 * 将 市场风险标准法风险暴露临时表 数据插入到 市场风险标准法风险暴露结果表
	 * @param db 数据库处理对象
	 * @return 影响记录数
	 * @throws EngineSQLException 数据库操作异常
	 */
	private int insertMarketExposure(DBConnection db) throws EngineSQLException {
		String sql = "INSERT INTO RWA_ERR_MarketExposureSTD" +
				"(ResultSerialNo, ExposureID, DataDate, DataNo, MIRRGRApproach, MORApproach, " +
				"BookType, InstrumentsID, InstrumentsType, OrgID, OrgName, OrgType, MarketRiskType, " +
				"InteRateRiskType, EquityRiskType, ExchangeRiskType, CommodityName, OptionRiskType, " +
				"IssuerID, IssuerName, IssuerType, IssuerSubType, IssuerRegistState, IssuerRCERating, " +
				"SMBFlag, UnderBondFlag, PaymentDate, SecuritiesType, BondIssueIntent, ClaimsLevel, " +
				"ReABSFlag, OriginatorFlag, SecuritiesERating, StockCode, StockMarket, ExchangeArea, " +
				"StructuralExpoFlag, OptionUnderlyingFlag, OptionUnderlyingName, OptionID, Volatility, " +
				"StartDate, DueDate, OriginalMaturity, ResidualM, NextRepriceDate, NextRepriceM, " +
				"RateType, CouponRate, ModifiedDuration, PositionType, Position, Currency, HedgeSpotFlag, " +
				"OptionType, OptionPremium, OptionValue, OptionPositionType, Delta, Gamma, Vega, CF, " +
				"UnderBondPosition, DeltaPosition, PriceSensitivity, IRRSRType, SRPR, MaturityTimeBand, " +
				"MaturityTimeBandType, MaturityTimeZone, MaturityRW, MaturityYC, DurationTimeBand, " +
				"DurationTimeBandType, DurationTimeZone, DurationYC, GRPR, SRCR, OptionSimpleFlag, " +
				"OptionSimpleScene, OptionSimpleCR, GammaSum, NetGammaEffect, VegaSum, GammaCR, VegaCR, " +
				"CLIENT_EXT_RATING_GROUP, SCRA_RESULT, INVESTMENT_GRADE_FLAG) " +
				"SELECT '" + TaskJBO.resultNo + "' AS ResultSerialNo, ExposureID, TO_DATE('" + TaskJBO.dataDate + 
				"','YYYY-MM-DD') AS DataDate, '" + TaskJBO.dataNo +  "' AS DataNo, MIRRGRApproach, MORApproach, " +
				"BookType, InstrumentsID, InstrumentsType, OrgID, OrgName, OrgType, MarketRiskType, " +
				"InteRateRiskType, EquityRiskType, ExchangeRiskType, CommodityName, OptionRiskType, " +
				"IssuerID, IssuerName, IssuerType, IssuerSubType, IssuerRegistState, IssuerRCERating, " +
				"SMBFlag, UnderBondFlag, PaymentDate, SecuritiesType, BondIssueIntent, ClaimsLevel, " +
				"ReABSFlag, OriginatorFlag, SecuritiesERating, StockCode, StockMarket, ExchangeArea, " +
				"StructuralExpoFlag, OptionUnderlyingFlag, OptionUnderlyingName, OptionID, Volatility, " +
				"StartDate, DueDate, OriginalMaturity, ResidualM, NextRepriceDate, NextRepriceM, " +
				"RateType, CouponRate, ModifiedDuration, PositionType, Position, Currency, HedgeSpotFlag, " +
				"OptionType, OptionPremium, OptionValue, OptionPositionType, Delta, Gamma, Vega, CF, " +
				"UnderBondPosition, DeltaPosition, PriceSensitivity, IRRSRType, SRPR, MaturityTimeBand, " +
				"MaturityTimeBandType, MaturityTimeZone, MaturityRW, MaturityYC, DurationTimeBand, " +
				"DurationTimeBandType, DurationTimeZone, DurationYC, GRPR, SRCR, OptionSimpleFlag, " +
				"OptionSimpleScene, OptionSimpleCR, GammaSum, NetGammaEffect, VegaSum, GammaCR, VegaCR, " +
				"CLIENT_EXT_RATING_GROUP, SCRA_RESULT, INVESTMENT_GRADE_FLAG " +
				"FROM RWA_ETC_MarketExposureSTD";
		return db.executeUpdate(sql);
	}
	
	/**
	 * 将 利率风险时段结果临时表 数据插入到 利率风险时段结果表
	 * @param db 数据库处理对象
	 * @return 影响记录数
	 * @throws EngineSQLException 数据库操作异常
	 */
	private int insertInterestRateTB(DBConnection db) throws EngineSQLException {
		String sql = "INSERT INTO RWA_ERR_InterestRateTB" +
				"(ResultSerialNo, Currency, TimeBandType, DataDate, DataNo, TimeZone, " +
				"BondLP, BondSP, RateLP, RateSP, GLP, GSP, RW, WLP, WSP, WHP, WNP, PR, VCR) " +
				"SELECT ResultSerialNo, Currency, TimeBandType, DataDate, DataNo, TimeZone, " +
				"BondLP, BondSP, RateLP, RateSP, GLP, GSP, RW, WLP, WSP, WHP, WNP, PR, VCR " +
				"FROM RWA_ETR_InterestRateTB";
		return db.executeUpdate(sql);
	}
	
	/**
	 * 将 利率风险时区结果临时表 数据插入到 利率风险时区结果表
	 * @param db 数据库处理对象
	 * @return 影响记录数
	 * @throws EngineSQLException 数据库操作异常
	 */
	private int insertInterestRateTZ(DBConnection db) throws EngineSQLException {
		String sql = "INSERT INTO RWA_ERR_InterestRateTZ" +
				"(ResultSerialNo, Currency, TimeZone1, TimeZone2, DataDate, DataNo, " +
				"WLP, WSP, WHP, WNP, RW, HCR) " +
				"SELECT ResultSerialNo, Currency, TimeZone1, TimeZone2, DataDate, DataNo, " +
				"WLP, WSP, WHP, WNP, RW, HCR " +
				"FROM RWA_ETR_InterestRateTZ";
		return db.executeUpdate(sql);
	}
	
	/**
	 * 将 利率一般风险结果临时表 数据插入到 利率一般风险结果表
	 * @param db 数据库处理对象
	 * @return 影响记录数
	 * @throws EngineSQLException 数据库操作异常
	 */
	private int insertInterestRateGR(DBConnection db) throws EngineSQLException {
		String sql = "INSERT INTO RWA_ERR_InterestRateGR" +
				"(ResultSerialNo, Currency, DataDate, DataNo, MIRRGRApproach, GVCR, " +
				"Zone11CR, Zone22CR, Zone33CR, Zone12CR, Zone23CR, Zone13CR, GHCR, TBWNP, TBCR, GRCR) " +
				"SELECT ResultSerialNo, Currency, DataDate, DataNo, MIRRGRApproach, GVCR, " +
				"Zone11CR, Zone22CR, Zone33CR, Zone12CR, Zone23CR, Zone13CR, GHCR, TBWNP, TBCR, GRCR " +
				"FROM RWA_ETR_InterestRateGR";
		return db.executeUpdate(sql);
	}
	
	/**
	 * 将 股票风险分类结果临时表 数据插入到 股票风险分类结果表
	 * @param db 数据库处理对象
	 * @return 影响记录数
	 * @throws EngineSQLException 数据库操作异常
	 */
	private int insertEquityType(DBConnection db) throws EngineSQLException {
		String sql = "INSERT INTO RWA_ERR_EquityType" +
				"(ResultSerialNo, ExchangeArea, EquityRiskType, DataDate, DataNo, GLP, GSP) " +
				"SELECT ResultSerialNo, ExchangeArea, EquityRiskType, DataDate, DataNo, GLP, GSP " +
				"FROM RWA_ETR_EquityType";
		return db.executeUpdate(sql);
	}
	
	/**
	 * 将 股票风险结果临时表 数据插入到 股票风险结果表
	 * @param db 数据库处理对象
	 * @return 影响记录数
	 * @throws EngineSQLException 数据库操作异常
	 */
	private int insertEquity(DBConnection db) throws EngineSQLException {
		String sql = "INSERT INTO RWA_ERR_Equity" +
				"(ResultSerialNo, ExchangeArea, DataDate, DataNo, " +
				"GLP, GSP, GP, NP, SRPR, GRPR, SRCR, GRCR, RC) " +
				"SELECT ResultSerialNo, ExchangeArea, DataDate, DataNo, " +
				"GLP, GSP, GP, NP, SRPR, GRPR, SRCR, GRCR, RC " +
				"FROM RWA_ETR_Equity";
		return db.executeUpdate(sql);
	}
	
	/**
	 * 将 外汇风险分类结果临时表 数据插入到 外汇风险分类结果表
	 * @param db 数据库处理对象
	 * @return 影响记录数
	 * @throws EngineSQLException 数据库操作异常
	 */
	private int insertExchangeType(DBConnection db) throws EngineSQLException {
		String sql = "INSERT INTO RWA_ERR_ExchangeType" +
				"(ResultSerialNo, ExchangeRiskType, OrgType, DataDate, DataNo, " +
				"GGLP, GGSP, GNP, GOLP, GOSP, ONP, GSLP, GSSP, SNP, GNSNP) " +
				"SELECT ResultSerialNo, ExchangeRiskType, OrgType, DataDate, DataNo, " +
				"GGLP, GGSP, GNP, GOLP, GOSP, ONP, GSLP, GSSP, SNP, GNSNP " +
				"FROM RWA_ETR_ExchangeType";
		return db.executeUpdate(sql);
	}
	
	/**
	 * 将 外汇风险结果临时表 数据插入到 外汇风险结果表
	 * @param db 数据库处理对象
	 * @return 影响记录数
	 * @throws EngineSQLException 数据库操作异常
	 */
	private int insertExchange(DBConnection db) throws EngineSQLException {
		String sql = "INSERT INTO RWA_ERR_Exchange" +
				"(ResultSerialNo, DataDate, DataNo, GFCNLP, GFCNSP, GFCP, GoldNP, GNEP, PR, RC) " +
				"SELECT ResultSerialNo, DataDate, DataNo, GFCNLP, GFCNSP, GFCP, GoldNP, GNEP, PR, RC " +
				"FROM RWA_ETR_Exchange";
		return db.executeUpdate(sql);
	}
	
	/**
	 * 将 商品风险结果临时表 数据插入到 商品风险结果表
	 * @param db 数据库处理对象
	 * @return 影响记录数
	 * @throws EngineSQLException 数据库操作异常
	 */
	private int insertCommodity(DBConnection db) throws EngineSQLException {
		String sql = "INSERT INTO RWA_ERR_Commodity" +
				"(ResultSerialNo, CommodityName, DataDate, DataNo, GLP, GSP, NP, GP, NPPR, GPPR, RC) " +
				"SELECT ResultSerialNo, CommodityName, DataDate, DataNo, GLP, GSP, NP, GP, NPPR, GPPR, RC " +
				"FROM RWA_ETR_Commodity";
		return db.executeUpdate(sql);
	}
	
	/**
	 * 将 市场风险结果临时表 数据插入到 市场风险结果表
	 * @param db 数据库处理对象
	 * @return 影响记录数
	 * @throws EngineSQLException 数据库操作异常
	 */
	private int insertMarket(DBConnection db) throws EngineSQLException {
		String sql = "INSERT INTO RWA_ERR_Market" +
				"(ResultSerialNo, DataDate, DataNo, TaskType, TaskID, ParamVerNo, SchemeID, " +
				"ConsolidateFlag, MRApproach, MIRRGRApproach, RWA, RC, GRCR, SRCR, ABSSRCR, " +
				"IRRSRCR, IRRGRCR, ERSRCR, ERGRCR, FERGRCR, CRGRCR, " +
				"ORGRCR, OSHSCR, OSLPCR, ODIROCR, ODEOCR, ODFEOCR, ODCOCR, IRR_CR, FER_CR, CR_CR, ER_CR) " +
				"SELECT ResultSerialNo, DataDate, DataNo, TaskType, TaskID, ParamVerNo, SchemeID, " +
				"ConsolidateFlag, MRApproach, MIRRGRApproach, RWA, RC, GRCR, SRCR, ABSSRCR, " +
				"IRRSRCR, IRRGRCR, ERSRCR, ERGRCR, FERGRCR, CRGRCR, " +
				"ORGRCR, OSHSCR, OSLPCR, ODIROCR, ODEOCR, ODFEOCR, ODCOCR, IRR_CR, FER_CR, CR_CR, ER_CR " +
				"FROM RWA_ETR_Market";
		return db.executeUpdate(sql);
	}

}
