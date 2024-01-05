package com.amarsoft.rwa.engine.me.step;

import com.amarsoft.rwa.engine.me.exception.EngineSQLException;
import com.amarsoft.rwa.engine.me.jbo.EngineJBO;
import com.amarsoft.rwa.engine.me.jbo.TaskJBO;
import com.amarsoft.rwa.engine.me.util.DBUtils;
import com.amarsoft.rwa.engine.me.util.db.DBConnection;
import com.amarsoft.rwa.engine.me.util.log.LogUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.Date;

/**
 * 步骤：数据加载
 * <br>将计算所需数据加载到临时表。
 * @author 陈庆
 * @version 1.0 2013-06-05
 */
@Slf4j
public class LoadDataStep implements Step {
	
	/** 并表机构的not in sql语句 */
//	private String sqlNotInSubOrg = "";

	/**
	 * 构造一个具有给定步骤类型的<code>LoadDataStep</code>对象。
	 * @param t 步骤类型
	 */
	public LoadDataStep(StepType t) {
		TaskJBO.currentStep = t;
		TaskJBO.currentStepTime = new Date();
	}

	/**
	 * 将计算所需数据加载到临时表。
	 * @throws Exception 
	 */
	public void execute() throws Exception {
		log.info(TaskJBO.currentStep + "开始运行...");
		// 数据加载服务
		DBConnection db = new DBConnection();
		// 数据库操作影响记录数
		int r = 0;
		try {
			// 创建数据库连接
			db.createConnection();

			// 记录数据加载步骤开始时间
			LogUtil.updateStepLogByStart(db);

			// 目前只支持标准法计算
			if ("01".equals(TaskJBO.marketApproach)) {
				//TODO 并表与未并表暂未考虑
				// 加载市场风险标准法风险暴露临时表
				r = this.loadMarketExposure(db);
				log.info("插入市场风险标准法风险暴露数据(" + r + ")");
				//TODO 数据检查
			}
			// 统计分析
			DBUtils.analyzeTable("RWA_ETC_MarketExposureSTD");
			// 记录数据加载步骤结束时间
			LogUtil.updateStepLogByOver(db);
		} finally {
			// 关闭数据库连接
			db.close();
		}
		log.info(TaskJBO.currentStep + "运行结束...");
	}
	
	/**
	 * 加载市场风险标准法风险暴露数据
	 * @param db 数据库处理对象
	 * @return 影响记录数
	 * @throws EngineSQLException 数据库操作异常
	 */
	public int loadMarketExposure(DBConnection db) throws EngineSQLException {
		String sql = "";
		// 从客户信息表取基础信息
		if(1 == EngineJBO.enableClientInfo){
			sql = "INSERT INTO RWA_ETC_MarketExposureSTD" +
					"(ExposureID, BookType, InstrumentsID, InstrumentsType, " +
					"OrgID, OrgName, OrgType, MarketRiskType, InteRateRiskType, " +
					"EquityRiskType, ExchangeRiskType, CommodityName, OptionRiskType, " +
					"IssuerID, IssuerName, IssuerType, IssuerSubType, IssuerRegistState, " +
					"IssuerRCERating, SMBFlag, UnderBondFlag, PaymentDate, SecuritiesType, " +
					"BondIssueIntent, ClaimsLevel, ReABSFlag, OriginatorFlag, " +
					"SecuritiesERating, StockCode, StockMarket, ExchangeArea, " +
					"StructuralExpoFlag, OptionUnderlyingFlag, OptionUnderlyingName, " +
					"OptionID, Volatility, StartDate, DueDate, OriginalMaturity, " +
					"ResidualM, NextRepriceDate, NextRepriceM, RateType, CouponRate, " +
					"ModifiedDuration, PositionType, Position, Currency," +
					"IssuerRCERatingGroup, ABSERatingGroupSTD, " +
					"HedgeSpotFlag, OptionType, OptionPremium, OptionValue, " +
					"OptionPositionType, Delta, Gamma, Vega, IRRSRType, SRPR, SCRA_RESULT, INVESTMENT_GRADE_FLAG) " +
					" SELECT t1.ExposureID, t1.BookType, t1.InstrumentsID, t1.InstrumentsType, " +
					"t1.OrgID, t1.OrgName, t1.OrgType, t1.MarketRiskType, t1.InteRateRiskType, " +
					"t1.EquityRiskType, t1.ExchangeRiskType, t1.CommodityName, t1.OptionRiskType, " +
					"t1.IssuerID, t5.client_name, null, t5.client_type, t5.regist_state, " +
					"t5.or_rating, t1.SMBFlag, t1.UnderBondFlag, t1.PaymentDate, t1.SecuritiesType, " +
					"t1.BondIssueIntent, t1.ClaimsLevel, t1.ReABSFlag, t1.OriginatorFlag, " +
					"t1.SecuritiesERating, t1.StockCode, t1.StockMarket, t1.ExchangeArea, " +
					"t1.StructuralExpoFlag, t1.OptionUnderlyingFlag, t1.OptionUnderlyingName, " +
					"t1.OptionID, t1.Volatility, t1.StartDate, t1.DueDate, t1.OriginalMaturity, " +
					"t1.ResidualM, t1.NextRepriceDate, t1.NextRepriceM, t1.RateType, t1.CouponRate, " +
					"t1.ModifiedDuration, t1.PositionType, t1.Position, t1.Currency," +
					"t2.RatingGroup AS IssuerRCERatingGroup, t3.RatingGroup AS ABSERatingGroupSTD, " +
					"t4.HedgeSpotFlag, t4.OptionType, t4.OptionPremium, t4.OptionValue, " +
					"t4.OptionPositionType, t4.Delta, t4.Gamma, t4.Vega, t1.IRRSRType, t1.SRPR, t5.SCRA_RESULT, t5.INVESTMENT_GRADE_FLAG " +
					"FROM RWA_EI_MarketExposureSTD t1 " +
					"LEFT JOIN RWA_EI_CLIENT t5 ON t5.DATA_BATCH_NO = t1.DataNo AND t5.CLIENT_ID = t1.ISSUERID " +
					"LEFT JOIN RWA_EP_RatingMapping t2 ON t2.RatingType = '01' and t2.RatingResult = t5.or_rating " +
					"LEFT JOIN RWA_EP_RatingMapping t3 ON t3.RatingType = '04' and t3.RatingResult = t1.SecuritiesERating " +
					"LEFT JOIN RWA_EI_MarketOptionSTD t4 ON t4.DataDate = t1.DataDate AND t4.DataNo = t1.DataNo AND t4.OptionID = t1.OptionID " +
					"WHERE t1.DataDate = TO_DATE('" + TaskJBO.dataDate +
					"','YYYY-MM-DD') AND t1.DataNo = '" + TaskJBO.dataNo + "' ";
		} else {
			sql = "INSERT INTO RWA_ETC_MarketExposureSTD" +
					"(ExposureID, BookType, InstrumentsID, InstrumentsType, " +
					"OrgID, OrgName, OrgType, MarketRiskType, InteRateRiskType, " +
					"EquityRiskType, ExchangeRiskType, CommodityName, OptionRiskType, " +
					"IssuerID, IssuerName, IssuerType, IssuerSubType, IssuerRegistState, " +
					"IssuerRCERating, SMBFlag, UnderBondFlag, PaymentDate, SecuritiesType, " +
					"BondIssueIntent, ClaimsLevel, ReABSFlag, OriginatorFlag, " +
					"SecuritiesERating, StockCode, StockMarket, ExchangeArea, " +
					"StructuralExpoFlag, OptionUnderlyingFlag, OptionUnderlyingName, " +
					"OptionID, Volatility, StartDate, DueDate, OriginalMaturity, " +
					"ResidualM, NextRepriceDate, NextRepriceM, RateType, CouponRate, " +
					"ModifiedDuration, PositionType, Position, Currency," +
					"IssuerRCERatingGroup, ABSERatingGroupSTD, " +
					"HedgeSpotFlag, OptionType, OptionPremium, OptionValue, " +
					"OptionPositionType, Delta, Gamma, Vega, IRRSRType, SRPR, SCRA_RESULT, INVESTMENT_GRADE_FLAG) " +
					" SELECT t1.ExposureID, t1.BookType, t1.InstrumentsID, t1.InstrumentsType, " +
					"t1.OrgID, t1.OrgName, t1.OrgType, t1.MarketRiskType, t1.InteRateRiskType, " +
					"t1.EquityRiskType, t1.ExchangeRiskType, t1.CommodityName, t1.OptionRiskType, " +
					"t1.IssuerID, t1.IssuerName, t1.IssuerType, t1.IssuerSubType, t1.IssuerRegistState, " +
					"t1.IssuerRCERating, t1.SMBFlag, t1.UnderBondFlag, t1.PaymentDate, t1.SecuritiesType, " +
					"t1.BondIssueIntent, t1.ClaimsLevel, t1.ReABSFlag, t1.OriginatorFlag, " +
					"t1.SecuritiesERating, t1.StockCode, t1.StockMarket, t1.ExchangeArea, " +
					"t1.StructuralExpoFlag, t1.OptionUnderlyingFlag, t1.OptionUnderlyingName, " +
					"t1.OptionID, t1.Volatility, t1.StartDate, t1.DueDate, t1.OriginalMaturity, " +
					"t1.ResidualM, t1.NextRepriceDate, t1.NextRepriceM, t1.RateType, t1.CouponRate, " +
					"t1.ModifiedDuration, t1.PositionType, t1.Position, t1.Currency," +
					"t2.RatingGroup AS IssuerRCERatingGroup, t3.RatingGroup AS ABSERatingGroupSTD, " +
					"t4.HedgeSpotFlag, t4.OptionType, t4.OptionPremium, t4.OptionValue, " +
					"t4.OptionPositionType, t4.Delta, t4.Gamma, t4.Vega, t1.IRRSRType, t1.SRPR, t5.SCRA_RESULT, t5.INVESTMENT_GRADE_FLAG " +
					"FROM RWA_EI_MarketExposureSTD t1 " +
					"LEFT JOIN RWA_EI_CLIENT t5 ON t5.DATA_BATCH_NO = t1.DataNo AND t5.CLIENT_ID = t1.ISSUERID " +
					"LEFT JOIN RWA_EP_RatingMapping t2 ON t2.RatingType = '01' and t2.RatingResult = t1.IssuerRCERating " +
					"LEFT JOIN RWA_EP_RatingMapping t3 ON t3.RatingType = '04' and t3.RatingResult = t1.SecuritiesERating " +
					"LEFT JOIN RWA_EI_MarketOptionSTD t4 ON t4.DataDate = t1.DataDate AND t4.DataNo = t1.DataNo AND t4.OptionID = t1.OptionID " +
					"WHERE t1.DataDate = TO_DATE('" + TaskJBO.dataDate +
					"','YYYY-MM-DD') AND t1.DataNo = '" + TaskJBO.dataNo + "' ";
		}
		return db.executeUpdate(sql);
	}

}
