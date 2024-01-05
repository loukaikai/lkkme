package com.amarsoft.rwa.engine.me.step;

import com.amarsoft.rwa.engine.me.exception.EngineDataException;
import com.amarsoft.rwa.engine.me.exception.EngineParameterException;
import com.amarsoft.rwa.engine.me.exception.EngineSQLException;
import com.amarsoft.rwa.engine.me.jbo.TaskJBO;
import com.amarsoft.rwa.engine.me.util.DBUtils;
import com.amarsoft.rwa.engine.me.util.EngineUtil;
import com.amarsoft.rwa.engine.me.util.db.DBConnection;
import com.amarsoft.rwa.engine.me.util.log.LogUtil;
import lombok.extern.slf4j.Slf4j;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 * 步骤：参数映射
 * <br>根据设定的参数，映射计算相关的风险参数。
 * 
 * @author 陈庆
 * @version 1.0 2013-06-05
 *
 */
@Slf4j
public class MappingParamsStep implements Step {
	
	
	/**
	 * 构造一个具有给定步骤类型的<code>MappingParamStep</code>对象。
	 * @param t 步骤类型
	 */
	public MappingParamsStep(StepType t) {
		TaskJBO.currentStep = t;
		TaskJBO.currentStepTime = new Date();
	}
	
	/**
	 * 根据设定的参数，映射计算相关的风险参数。
	 * @throws Exception
	 */
	public void execute() throws Exception {
		log.info(TaskJBO.currentStep.getName() + "步骤开始");
		// 数据库连接
		DBConnection db = new DBConnection();
		try {
			// 创建数据库连接
			db.createConnection();
			
			// 更新步骤日志
			LogUtil.updateStepLogByStart(db);
			
			// 目前只支持标准法
			if ("01".equals(TaskJBO.marketApproach)) {
				/* 获取映射参数 */
				Map<String, List<Map<String, String>>> mapping = this.getParamMapping(db, TaskJBO.marketParamVerNo,null);
				log.info("获取监管映射参数");
				
				// 映射承销债券转换系数
				this.mappingUnderBondCF(db, mapping.get("UBCF"));
				log.info("承销债券转换系数映射结束");
				
				// 计提比率映射
				this.mappingSRPROfGenarel(db, mapping.get("SRPR"));
				log.info("政府证券与合格证券计提比率映射结束");
				
				// 资产证券化计提比率映射
				// TODO 暂时不考虑，巴二和巴三差异比较大
		/*		this.mappingSRPROfAbs(db, mapping.get("ABSRW"));
				log.info("资产证券化计提比率映射结束");*/
				
				// 其他证券计提比率映射
				// 巴三在计提比率映射已经考虑
/*				this.mappingSRPROfOther(db, mapping.get("STDRW"));
				log.info("其他证券计提比率映射结束");*/
				
				// 到期日法时段时区映射
				this.mappingMaturityTime(db, mapping.get("MT"));
				log.info("到期日法时段时区映射结束");
				
				// 到期日法时段权重、收益率变化映射
				this.mappingMaturityRWYC(db, mapping.get("TBRW"));
				log.info("到期日法时段权重映射结束");
				
				// 利率风险计算方法为 久期法，需进行久期法映射
				if ("010102".equals(TaskJBO.mirrgrApproach)) {
					// 时段时区映射
					this.mappingDurationTime(db, mapping.get("DT"));
					log.info("久期法时段时区映射结束");
					
					// 时段收益率变化映射
					this.mappingDurationYC(db, mapping.get("TBYC"));
					log.info("久期法时段收益率变化映射结束");
				}
				
				// 承销债券转换系数
				this.checkDataByCF(db);
				// 债券特定风险计提比率
				this.checkDataBySRPR(db);
				// 到期日 时段、时区、权重、收益率变化
				this.checkDataByMaturity(db);
				// 久期法 时段、时区、收益率变化
				if ("010102".equals(TaskJBO.mirrgrApproach)) {
					this.checkDataByDuration(db);
				}
			}
			// 统计分析
			DBUtils.analyzeTable("RWA_ETC_MarketExposureSTD");
			// 更新步骤日志
			LogUtil.updateStepLogByOver(db);
		} finally {
			// 关闭连接
			db.close();
		}
		log.info(TaskJBO.currentStep.getName() + "步骤结束");
	}

	/**
	 * 获取映射参数集合
	 * @param db 数据库处理对象
	 * @param marketParamVerNo 市场风险参数版本流水号
	 * @param creditParamVerNo 信用风险参数版本流水号
	 * @return 映射参数集合
	 * @throws EngineSQLException 数据库操作异常
	 * @throws EngineParameterException 参数异常
	 */
	private Map<String, List<Map<String, String>>> getParamMapping(DBConnection db, 
			String marketParamVerNo, String creditParamVerNo) throws EngineSQLException, EngineParameterException {
		Map<String, List<Map<String, String>>> mapping = new HashMap<String, List<Map<String, String>>>();
		
		// 获取资产风险权重 巴三不在需要
		//mapping.put("STDRW", this.getSTDRW(db, creditParamVerNo));
		
		// 获取资产证券化标准法风险权重
		// TODO abs权重映射巴二和巴三不一致，部分银行直接补录权重，暂时考虑
		//mapping.put("ABSRW", this.getABSRW(db, marketParamVerNo));
		
		// 承销债券转换系数
		mapping.put("UBCF", this.getUnderBondCF(db, marketParamVerNo));
		
		// 利率特定风险计提比率
		mapping.put("SRPR", this.getIRRSRPR(db, marketParamVerNo));
		
		// 获取到期日法时段时区
		mapping.put("MT", this.getMaturityTime(db, marketParamVerNo));
		
		// 获取久期法时段时区
		mapping.put("DT", this.getDurationTime(db, marketParamVerNo));
		
		// 获取时段权重
		mapping.put("TBRW", this.getTimeBandRW(db, marketParamVerNo));
		
		// 获取时段收益率变化
		mapping.put("TBYC", this.getTimeBandYC(db, marketParamVerNo));
		return mapping;
	}
	
	/**
	 * 根据参数版本流水号读取资产风险权重表中的资产风险权重集合
	 * <br>只需获取一般资产的风险权重映射集合
	 * @param db 数据库处理对象
	 * @param paramVerNo 参数版本流水号
	 * @return list 资产风险权重列表
	 * @throws EngineSQLException 数据库操作异常
	 */
	private List<Map<String, String>> getSTDRW(DBConnection db, String paramVerNo) throws EngineSQLException {
		List<Map<String, String>> list = new ArrayList<Map<String, String>>();
		Map<String, String> m = null;
		String sql = "SELECT ExpoClassSTD, ExpoSubClassSTD, BusinessTypeSTD, ClientType, ClientSubType, RegistState,"
				+ " RCERatingGroup, SSMBFlag, ExpoOriginalM, ClaimsLevel, BondFlag, BondIssueIntent, RW "
				+ " FROM RWA_EP_RWAsset WHERE ParamVerNo = '" + paramVerNo + "' AND BusinessTypeSTD = '07'";
		Statement st = null;
		ResultSet rs = null;
		try {
			st = db.createStatement();
			rs = db.executeQuery(st, sql);
			while (rs.next()) {
				m = new HashMap<String, String>();
				m.put("ExpoClassSTD", rs.getString("ExpoClassSTD"));	// 权重法暴露大类
				m.put("ExpoSubClassSTD", rs.getString("ExpoSubClassSTD"));	// 权重法暴露小类
				m.put("BusinessTypeSTD", rs.getString("BusinessTypeSTD"));	// 权重法业务类型
				m.put("ClientType", rs.getString("ClientType"));	// 参与主体大类
				m.put("ClientSubType", rs.getString("ClientSubType"));	// 参与主体小类
				m.put("RegistState", rs.getString("RegistState"));	// 注册国家或地区
				m.put("RCERatingGroup", rs.getString("RCERatingGroup"));	// 境外注册地外部评级范围
				m.put("SSMBFlag", rs.getString("SSMBFlag"));	// 标准微小企业标识
				m.put("ExpoOriginalM", rs.getString("ExpoOriginalM"));	// 暴露原始期限
				m.put("ClaimsLevel", rs.getString("ClaimsLevel"));	// 债权级别
				m.put("BondFlag", rs.getString("BondFlag"));	// 是否为债券
				m.put("BondIssueIntent", rs.getString("BondIssueIntent"));	// 债券发行目的
				m.put("RW", EngineUtil.format(rs.getDouble("RW")));	// 风险权重
				list.add(m);
			}
		} catch (SQLException e) {
			throw new EngineSQLException("结果集获取资产风险权重列表异常[信用风险参数版本流水号：" + paramVerNo + "]", e);
		} finally {
			db.closeResultSet(rs);
			db.closeStatement(st);
		}
		return list;
	}
	
	/**
	 * 获取资产证券化标准法风险权重参数
	 * @param db 数据库处理对象
	 * @param paramVerNo 参数版本流水号
	 * @return 返回资产证券化标准法风险权重参数列表
	 * @throws EngineSQLException 数据库操作异常
	 */
	public List<Map<String, String>> getABSRW(DBConnection db, String paramVerNo) throws EngineSQLException {
		List<Map<String, String>> list = new ArrayList<Map<String, String>>();
		Map<String, String> m = null;
		String sql = "SELECT ParamSerialNo, ReABSEFlag, OriginatorFlag, ERatingGroupSTD, RecoERatingFlag, RW " +
				"FROM RWA_EP_RWABSSTD WHERE paramVerNo = '" + paramVerNo + "'";
		Statement st = null;
		ResultSet rs = null;
		try {
			st = db.createStatement();
			rs = db.executeQuery(st, sql);
			while (rs.next()) {
				m = new HashMap<String, String>();
				m.put("ParamSerialNo", rs.getString("ParamSerialNo"));	// 参数流水号
				m.put("ReABSEFlag", rs.getString("ReABSEFlag"));// 再资产证券化暴露标识
				m.put("OriginatorFlag", rs.getString("OriginatorFlag"));// 是否发起机构
				m.put("ERatingGroupSTD", rs.getString("ERatingGroupSTD"));// 标准法外部评级范围
				m.put("RecoERatingFlag", rs.getString("RecoERatingFlag"));// 是否有认可外部评级
				m.put("RW", EngineUtil.format(rs.getDouble("RW")));// 风险权重
				list.add(m);
			}
		} catch (SQLException e) {
			throw new EngineSQLException("结果集获取资产证券化标准法风险权重参数异常[信用风险参数版本流水号：" + paramVerNo + "]", e);
		} finally {
			db.closeResultSet(rs);
			db.closeStatement(st);
		}
		return list;
	}
	
	/**
	 * 根据参数版本流水号读取承销债券转换系数表中的承销债券转换系数集合
	 * @param db 数据库处理对象
	 * @param paramVerNo 参数版本流水号
	 * @return list 承销债券转换系数列表
	 * @throws EngineSQLException 数据库操作异常
	 */
	public List<Map<String, String>> getUnderBondCF(DBConnection db, String paramVerNo)
			throws EngineSQLException {
		List<Map<String, String>> list = new ArrayList<Map<String, String>>();
		Map<String, String> m = null;
		String sql = "Select ParamSerialNo, CFConfirm, CF "
				+ "From RWA_EP_UnderBondCF Where ParamVerNo = '" + paramVerNo + "'";
		Statement st = null;
		ResultSet rs = null;
		try {
			st = db.createStatement();
			rs = db.executeQuery(st, sql);
			while (rs.next()) {
				m = new HashMap<String, String>();
				m.put("ParamSerialNo", rs.getString("ParamSerialNo"));	// 参数流水号
				m.put("CFConfirm", rs.getString("CFConfirm"));	// 转换系数确定方式
				m.put("CF", EngineUtil.format(rs.getDouble("CF")));	// 转换系数
				list.add(m);
			}
		} catch (SQLException e) {
			throw new EngineSQLException("结果集获取承销债券转换系数列表异常[参数版本流水号：" + paramVerNo + "]", e);
		} finally {
			db.closeResultSet(rs);
			db.closeStatement(st);
		}
		return list;
	}
	
	/**
	 * 根据参数版本流水号读取特定市场风险计提比率表中的特定市场风险计提比率集合
	 * @param db 数据库处理对象
	 * @param paramVerNo 参数版本流水号
	 * @return list 特定市场风险计提比率列表
	 * @throws EngineSQLException 数据库操作异常 
	 */
	public List<Map<String, String>> getIRRSRPR(DBConnection db, String paramVerNo)
			throws EngineSQLException {
		List<Map<String, String>> list = new ArrayList<Map<String, String>>();
		Map<String, String> m = null;
		String sql = "Select ParamSerialNo, IRRSRType, SecuritiesType, IssuerType, IssuerSubType, "
				+ "IssuerRegistState, IssuerRCERatingGroup, SecuritiesRMType, CLIENT_EXT_RATING_GROUP, SCRA_RESULT, INVESTMENT_GRADE_FLAG, PR "
				+ "From RWA_EP_IRRSRPR Where ParamVerNo = '" + paramVerNo + "'";
		Statement st = null;
		ResultSet rs = null;
		try {
			st = db.createStatement();
			rs = db.executeQuery(st, sql);
			while (rs.next()) {
				m = new HashMap<String, String>();
				m.put("ParamSerialNo", rs.getString("ParamSerialNo")); // 参数流水号
				m.put("IRRSRType", rs.getString("IRRSRType")); // 利率特定风险分类
				m.put("SecuritiesType", rs.getString("SecuritiesType")); // 证券类别
				//m.put("IssuerType", rs.getString("IssuerType")); // 发行人大类
				m.put("IssuerSubType", rs.getString("IssuerSubType")); // 发行人小类
				m.put("IssuerRegistState", rs.getString("IssuerRegistState")); // 发行人注册国家
				m.put("IssuerRCERatingGroup", rs.getString("IssuerRCERatingGroup")); // 发行人外部评级范围
				m.put("SecuritiesRMType", rs.getString("SecuritiesRMType")); // 证券剩余期限类型
				m.put("clientExtRatingGroup", rs.getString("CLIENT_EXT_RATING_GROUP")); // 参与主体外部评级范围
				m.put("scraResult", rs.getString("SCRA_RESULT")); // 标准信用风险评估结果
				m.put("investmentGradeFlag", rs.getString("INVESTMENT_GRADE_FLAG")); // 投资级标识
				m.put("PR", EngineUtil.format(rs.getDouble("PR"))); // 计提比例
				list.add(m);
			}
		} catch (SQLException e) {
			throw new EngineSQLException("结果集获取特定市场风险计提比率列表异常[参数版本流水号：" + paramVerNo + "]", e);
		} finally {
			db.closeResultSet(rs);
			db.closeStatement(st);
		}
		return list;
	}
	
	/**
	 * 获取到期日法时段时区参数
	 * @param db 数据库处理对象
	 * @param paramVerNo 参数版本流水号
	 * @return 返回到期日法时段时区参数列表
	 * @throws EngineSQLException 数据库操作异常
	 */
	public List<Map<String, String>> getMaturityTime(DBConnection db,
			String paramVerNo) throws EngineSQLException {
		List<Map<String, String>> list = new ArrayList<Map<String, String>>();
		Map<String, String> m = null;
		String sql = "Select ParamSerialNo, RMLimit, CouponRateType, TimeBand, TimeBandType, TimeZone "
				+ "From RWA_EP_MaturityTime WHERE ParamVerNo = '" + paramVerNo + "' Order by RMLimit";
		Statement st = null;
		ResultSet rs = null;
		try {
			st = db.createStatement();
			rs = db.executeQuery(st, sql);
			while (rs.next()) {
				m = new HashMap<String, String>();
				m.put("ParamSerialNo", rs.getString("ParamSerialNo")); // 参数流水号
				m.put("RMLimit", EngineUtil.format(rs.getDouble("RMLimit"))); // 期限上限
				m.put("CouponRateType", rs.getString("CouponRateType")); // 票面利率类型
				m.put("TimeBand", rs.getString("TimeBand")); // 时段
				m.put("TimeBandType", rs.getString("TimeBandType")); // 时段分类
				m.put("TimeZone", rs.getString("TimeZone")); // 时区
				list.add(m);
			}
		} catch (SQLException e) {
			throw new EngineSQLException("结果集获取到期日法时段时区参数异常", e);
		} finally {
			db.closeResultSet(rs);
			db.closeStatement(st);
		}
		return list;
	}
	
	/**
	 * 获取久期法时段时区参数
	 * 
	 * @param db 数据库处理对象
	 * @param paramVerNo 参数版本流水号
	 * @return 返回久期法时段时区参数列表
	 * @throws EngineSQLException 数据库操作异常
	 */
	public List<Map<String, String>> getDurationTime(DBConnection db,
			String paramVerNo) throws EngineSQLException {
		List<Map<String, String>> list = new ArrayList<Map<String, String>>();
		Map<String, String> m = null;
		String sql = "Select ParamSerialNo, RMLimit, TimeBand, TimeZone, TimeBandType " +
				"From RWA_EP_DurationTime WHERE ParamVerNo = '" + paramVerNo + "' Order by RMLimit";
		Statement st = null;
		ResultSet rs = null;
		try {
			st = db.createStatement();
			rs = db.executeQuery(st, sql);
			while (rs.next()) {
				m = new HashMap<String, String>();
				m.put("ParamSerialNo", rs.getString("ParamSerialNo")); // 参数流水号
				m.put("RMLimit", EngineUtil.format(rs.getDouble("RMLimit"))); // 期限上限
				m.put("TimeBand", rs.getString("TimeBand")); // 时段
				m.put("TimeBandType", rs.getString("TimeBandType")); // 时段分类
				m.put("TimeZone", rs.getString("TimeZone")); // 时区
				list.add(m);
			}
		} catch (SQLException e) {
			throw new EngineSQLException("结果集获取久期法时段时区参数异常", e);
		} finally {
			db.closeResultSet(rs);
			db.closeStatement(st);
		}
		return list;
	}
	
	/**
	 * 获取时段权重参数
	 * 
	 * @param db 数据库处理对象
	 * @param paramVerNo 参数版本流水号
	 * @return 返回时段权重参数列表
	 * @throws EngineSQLException 数据库操作异常
	 */
	public List<Map<String, String>> getTimeBandRW(DBConnection db,
			String paramVerNo) throws EngineSQLException {
		List<Map<String, String>> list = new ArrayList<Map<String, String>>();
		Map<String, String> m = null;
		String sql = "Select ParamSerialNo, TimeBandType, RW, YC " +
				"From RWA_EP_TimeBandRW WHERE ParamVerNo = '" + paramVerNo + "'";
		Statement st = null;
		ResultSet rs = null;
		try {
			st = db.createStatement();
			rs = db.executeQuery(st, sql);
			while (rs.next()) {
				m = new HashMap<String, String>();
				m.put("ParamSerialNo", rs.getString("ParamSerialNo"));	// 参数流水号
				m.put("TimeBandType", rs.getString("TimeBandType")); // 时段分类
				m.put("RW", EngineUtil.format(rs.getDouble("RW"))); // 权重
				m.put("YC", EngineUtil.format(rs.getDouble("YC"))); // 收益率变化
				list.add(m);
			}
		} catch (SQLException e) {
			throw new EngineSQLException("结果集获取时段权重参数异常", e);
		} finally {
			db.closeResultSet(rs);
			db.closeStatement(st);
		}
		return list;
	}
	
	/**
	 * 获取时段收益率变化表参数
	 * 
	 * @param db 数据库处理对象
	 * @param paramVerNo 参数版本流水号
	 * @return 返回时段收益率变化表参数列表
	 * @throws EngineSQLException 数据库操作异常
	 */
	public List<Map<String, String>> getTimeBandYC(DBConnection db,
			String paramVerNo) throws EngineSQLException {
		List<Map<String, String>> list = new ArrayList<Map<String, String>>();
		Map<String, String> m = null;
		String sql = "Select ParamSerialNo, TimeBandType, YC " +
				"From RWA_EP_TimeBandYC WHERE ParamVerNo = '" + paramVerNo + "'";
		Statement st = null;
		ResultSet rs = null;
		try {
			st = db.createStatement();
			rs = db.executeQuery(st, sql);
			while (rs.next()) {
				m = new HashMap<String, String>();
				m.put("ParamSerialNo", rs.getString("ParamSerialNo"));	// 参数流水号
				m.put("TimeBandType", rs.getString("TimeBandType")); // 时段分类
				m.put("YC", EngineUtil.format(rs.getDouble("YC"))); // 收益率变化
				list.add(m);
			}
		} catch (SQLException e) {
			throw new EngineSQLException("结果集获取时段收益率变化表参数异常", e);
		} finally {
			db.closeResultSet(rs);
			db.closeStatement(st);
		}
		return list;
	}
	
	/**
	 * 利率风险暴露临时表中承销债券转换系数映射
	 * @param db 数据库处理对象
	 * @param list 映射参数列表
	 * @throws EngineSQLException 数据库操作异常
	 * @throws EngineParameterException 参数异常
	 */
	public void mappingUnderBondCF(DBConnection db, List<Map<String, String>> list) 
			throws EngineSQLException, EngineParameterException {
		String sql = "";
		String tempSql = "";
//		String date = EngineUtil.parseDate(TaskJBO.dataDate);
		for (Map<String, String> m : list) {
			// 确认方式 :01 缴款日之前, 02 缴款日之后
			if (m.get("CFConfirm") == null) {
				throw new EngineParameterException("承销债券转换系数表参数错误,转换系数确认方式不能为空！");
			} else if ("01".equals(m.get("CFConfirm"))) {
				tempSql = " And Paymentdate > '" + TaskJBO.dataDate + "'";
			} else if ("02".equals(m.get("CFConfirm"))) {
				tempSql = " And PaymentDate <= '" + TaskJBO.dataDate + "'";
			} else {
				throw new EngineParameterException("承销债券转换系数表参数错误,转换系数确认方式值域仅限01和02！");
			}
			sql = "Update RWA_ETC_MarketExposureSTD Set CF = " + m.get("CF")
					+ " Where MarketRiskType = '01' And UnderBondFlag = '1' " + tempSql;
			db.executeUpdate(sql);
		}
	}
	
	/**
	 * 利率风险暴露临时表中政府证券与合格证券的特定市场风险计提比率映射
	 * @param db 数据库处理对象
	 * @param list 映射参数列表
	 * @throws EngineSQLException 数据库操作异常
	 * @throws EngineParameterException 参数异常
	 */
	public void mappingSRPROfGenarel(DBConnection db, List<Map<String, String>> list) 
			throws EngineSQLException, EngineParameterException {
		String sql = "";
		String tempSql = "";
		for (Map<String, String> m : list) {
			// 债券剩余期限:01 <=6月,02 6-24月, 03 >24月
			if (m.get("SecuritiesRMType") == null || "".equals(m.get("SecuritiesRMType"))) {
				tempSql = "";
			} else if ("01".equals(m.get("SecuritiesRMType"))) {
				tempSql = " AND ResidualM <= 0.5 ";
			} else if ("02".equals(m.get("SecuritiesRMType"))) {
				tempSql = " AND ResidualM > 0.5 AND ResidualM <= 2 ";
			} else if ("03".equals(m.get("SecuritiesRMType"))) {
				tempSql = " AND ResidualM > 2 ";
			} else {
				throw new EngineParameterException("特定市场风险计提比率表参数错误,债券剩余期限类型值域仅限01、02、03！");
			}
			sql = "Update RWA_ETC_MarketExposureSTD Set IRRSRType = '" + m.get("IRRSRType")
					+ "', SRPR = " + m.get("PR")
					+ " Where MarketRiskType = '01' And InteRateRiskType = '01' And IRRSRType is null " // 如果如果上游给了使用上游数据，如果没有给使用系统映射
					+ EngineUtil.getSqlAnd("SecuritiesType", m.get("SecuritiesType"))
					// + EngineUtil.getSqlAnd("IssuerType", m.get("IssuerType"))
					+ EngineUtil.getSqlAnd("IssuerSubType", m.get("IssuerSubType"))
					+ EngineUtil.getSqlAnd("IssuerRegistState", m.get("IssuerRegistState"))
					+ EngineUtil.getSqlAnd("IssuerRCERatingGroup", m.get("IssuerRCERatingGroup"))
					+ EngineUtil.getSqlAnd("INVESTMENT_GRADE_FLAG", m.get("investmentGradeFlag"))
					+ EngineUtil.getSqlAnd("SCRA_RESULT", m.get("scraResult"))
					+ EngineUtil.getSqlAnd("CLIENT_EXT_RATING_GROUP", m.get("clientExtRatingGroup"))
					+ tempSql;
			db.executeUpdate(sql);
		}
	}
	
	/**
	 * 利率风险暴露临时表中资产证券化的特定市场风险计提比率映射
	 * @param db 数据库处理对象
	 * @param list 映射参数列表
	 * @throws EngineSQLException 数据库操作异常
	 * @throws EngineParameterException 参数异常
	 */
	public void mappingSRPROfAbs(DBConnection db, List<Map<String, String>> list) 
			throws EngineSQLException, EngineParameterException {
		String sql = "";
		for (Map<String, String> m : list) {
			// 有认可外部评级
			if (m.get("RecoERatingFlag") == null || "".equals(m.get("RecoERatingFlag"))) {
				throw new EngineParameterException("资产证券化标准法风险权重表参数错误,是否有认可外部评级 必须设置！");
			} else if (m.get("RecoERatingFlag") != null && "0".equals(m.get("RecoERatingFlag"))) {
				continue;
			}
			sql = "Update RWA_ETC_MarketExposureSTD Set IRRSRType = SecuritiesType, SRPR = " + (Double.parseDouble(m.get("RW")) / 12.5)
					+ " Where MarketRiskType = '01' And InteRateRiskType = '01' And SecuritiesType = '03' "
					+ EngineUtil.getSqlAnd("ReABSFlag", m.get("ReABSEFlag")) 
					+ EngineUtil.getSqlAnd("OriginatorFlag", m.get("OriginatorFlag")) 
					+ EngineUtil.getSqlAnd("ABSERatingGroupSTD", m.get("ERatingGroupSTD"));
			db.executeUpdate(sql);
		}
	}
	
	/**
	 * 利率风险暴露临时表中其他证券的特定市场风险计提比率映射
	 * @param db 数据库处理对象
	 * @param list 映射参数列表
	 * @throws EngineSQLException 数据库操作异常
	 * @throws EngineParameterException 参数异常
	 */
	public void mappingSRPROfOther(DBConnection db, List<Map<String, String>> list) 
			throws EngineSQLException, EngineParameterException {
		String sql = "";
		String tempSql = "";
		for (Map<String, String> m : list) {
			// 默认为债券
			if (m.get("BondFlag") != null && "0".equals(m.get("BondFlag"))) {
				continue;
			}
			// 债券原始期限:01 <=3月,02  >3月
			if (m.get("ExpoOriginalM") == null || "".equals(m.get("ExpoOriginalM"))) {
				tempSql = "";
			} else if ("01".equals(m.get("ExpoOriginalM"))) {
				tempSql = " AND OriginalMaturity <= 0.25 ";
			} else if ("02".equals(m.get("ExpoOriginalM"))) {
				tempSql = " AND OriginalMaturity > 0.25 ";
			} else {
				throw new EngineParameterException("资产风险权重映射表参数错误,暴露原始期限值域仅限01和02！");
			}
			sql = "Update RWA_ETC_MarketExposureSTD Set IRRSRType = SecuritiesType, SRPR = " + (Double.parseDouble(m.get("RW")) / 12.5)
					+ " Where MarketRiskType = '01' And InteRateRiskType = '01' And SecuritiesType = '09' "
					+ EngineUtil.getSqlAnd("IssuerType", m.get("ClientType"))
					+ EngineUtil.getSqlAnd("IssuerSubType", m.get("ClientSubType"))
					+ EngineUtil.getSqlAnd("IssuerRegistState", m.get("RegistState"))
					+ EngineUtil.getSqlAnd("IssuerRCERatingGroup", m.get("RCERatingGroup"))
					+ EngineUtil.getSqlAnd("SMBFlag", m.get("SSMBFlag"))
					+ EngineUtil.getSqlAnd("ClaimsLevel", m.get("ClaimsLevel"))
					+ EngineUtil.getSqlAnd("BondIssueIntent", m.get("BondIssueIntent"))
					+ tempSql;
			db.executeUpdate(sql);
		}
	}
	
	/**
	 * 利率风险暴露临时表中所属到期日法时段时区映射
	 * @param db 数据库处理对象
	 * @param list 映射参数列表
	 * @throws EngineSQLException 数据库操作异常
	 * @throws EngineParameterException 参数异常
	 */
	public void mappingMaturityTime(DBConnection db, List<Map<String, String>> list) 
			throws EngineSQLException, EngineParameterException {
		String sql = "";
		String tempSql = "";
		double rm = 0;
		List<Map<String, String>> tempList = new ArrayList<Map<String,String>>();
		for (Map<String, String> m : list) {
			rm = Double.parseDouble(m.get("RMLimit"));
			// 期限上限为0 即为剩余情况
			if (rm == 0) {
				tempList.add(m);
				continue;
			}
			// 票面利率类型
			if (m.get("CouponRateType") == null) {
				throw new EngineParameterException("到期日法时段时区参数错误,票面利率类型不能为空！");
			} else if ("01".equals(m.get("CouponRateType"))) {
				tempSql = " And CouponRate >= 0.03 ";
			} else if ("02".equals(m.get("CouponRateType"))) {
				tempSql = " And CouponRate < 0.03 ";
			} else {
				throw new EngineParameterException("到期日法时段时区参数错误,票面利率类型值域仅限01和02！");
			}
			// 利率类型判断 RateType
			sql = "Update RWA_ETC_MarketExposureSTD Set MaturityTimeBand = '" + m.get("TimeBand")
					+ "', MaturityTimeBandType = '" + m.get("TimeBandType")
					+ "', MaturityTimeZone = '" + m.get("TimeZone")
					+ "' Where MarketRiskType = '01' And MaturityTimeBand is NULL " +
					"And (CASE WHEN RateType = '01' THEN ResidualM ELSE NextRepriceM END) <= " + rm + tempSql;
			db.executeUpdate(sql);
		}
		// 期限上限为0的剩余情况
		for (Map<String, String> m : tempList) {
			// 票面利率类型
			if (m.get("CouponRateType") == null) {
				throw new EngineParameterException("到期日法时段时区参数错误,票面利率类型不能为空！");
			} else if ("01".equals(m.get("CouponRateType"))) {
				tempSql = " And CouponRate >= 0.03 ";
			} else if ("02".equals(m.get("CouponRateType"))) {
				tempSql = " And CouponRate < 0.03 ";
			} else {
				throw new EngineParameterException("到期日法时段时区参数错误,票面利率类型值域仅限01和02！");
			}
			sql = "Update RWA_ETC_MarketExposureSTD Set MaturityTimeBand = '" + m.get("TimeBand")
					+ "', MaturityTimeBandType = '" + m.get("TimeBandType")
					+ "', MaturityTimeZone = '" + m.get("TimeZone")
					+ "' Where MarketRiskType = '01' And MaturityTimeBand is NULL " +
					"And (CASE WHEN RateType = '01' THEN ResidualM ELSE NextRepriceM END) > 0 " + tempSql;
			db.executeUpdate(sql);
		}
	}
	
	/**
	 * 利率风险暴露临时表中到期日法风险权重、收益率变化映射
	 * @param db 数据库处理对象
	 * @param list 映射参数列表
	 * @throws EngineSQLException 数据库操作异常
	 * @throws EngineParameterException 参数异常
	 */
	public void mappingMaturityRWYC(DBConnection db, List<Map<String, String>> list) 
			throws EngineSQLException, EngineParameterException {
		String sql = "";
		for (Map<String, String> m : list) {
			sql = "Update RWA_ETC_MarketExposureSTD Set MaturityRW = " + m.get("RW")
					+ ", MaturityYC = " + m.get("YC") 
					+ " Where MarketRiskType = '01' And MaturityTimeBandType = '" + m.get("TimeBandType") + "'";
			db.executeUpdate(sql);
		}
	}
	
	/**
	 * 利率风险暴露临时表中所属久期法时段时区映射
	 * @param db 数据库处理对象
	 * @param list 映射参数列表
	 * @throws EngineSQLException 数据库操作异常
	 * @throws EngineParameterException 参数异常
	 */
	public void mappingDurationTime(DBConnection db, List<Map<String, String>> list) 
			throws EngineSQLException, EngineParameterException {
		String sql = "";
		double rm = 0;
		List<Map<String, String>> tempList = new ArrayList<Map<String,String>>();
		for (Map<String, String> m : list) {
			rm = Double.parseDouble(m.get("RMLimit"));
			// 期限上限为0 即为剩余情况
			if (rm == 0) {
				tempList.add(m);
				continue;
			}
			// 久期法 根据 修正久期 分时段
			sql = "Update RWA_ETC_MarketExposureSTD Set DurationTimeBand = '" + m.get("TimeBand")
					+ "', DurationTimeBandType = '" + m.get("TimeBandType")
					+ "', DurationTimeZone = '" + m.get("TimeZone")
					+ "' Where MarketRiskType = '01' And MaturityTimeBand is NULL And ModifiedDuration <= " + rm;
			db.executeUpdate(sql);
		}
		// 期限上限为0的剩余情况
		for (Map<String, String> m : tempList) {
			sql = "Update RWA_ETC_MarketExposureSTD Set DurationTimeBand = '" + m.get("TimeBand")
					+ "', DurationTimeBandType = '" + m.get("TimeBandType")
					+ "', DurationTimeZone = '" + m.get("TimeZone")
					+ "' Where MarketRiskType = '01' And MaturityTimeBand is NULL And ModifiedDuration > 0";
			db.executeUpdate(sql);
		}
	}
	
	/**
	 * 利率风险暴露临时表中收益率变化映射
	 * @param db 数据库处理对象
	 * @param list 映射参数列表
	 * @throws EngineSQLException 数据库操作异常
	 * @throws EngineParameterException 参数异常
	 */
	public void mappingDurationYC(DBConnection db, List<Map<String, String>> list) 
			throws EngineSQLException, EngineParameterException {
		String sql = "";
		for (Map<String, String> m : list) {
			sql = "Update RWA_ETC_MarketExposureSTD Set DurationYC = " + m.get("YC")
					+ " Where MarketRiskType = '01' And DurationTimeBandType = '" + m.get("TimeBandType") + "'";
			db.executeUpdate(sql);
		}
	}
	
	/**
	 * 承销债券转换系数校验，若有空值则抛出数据异常
	 * @param db 数据库操作对象
	 * @throws EngineSQLException 数据库操作异常
	 * @throws EngineDataException 数据异常
	 */
	private void checkDataByCF(DBConnection db) throws EngineSQLException, EngineDataException {
		String sql = "SELECT COUNT(1) FROM RWA_ETC_MarketExposureSTD " +
				"Where MarketRiskType = '01' And UnderBondFlag = '1' AND CF IS NULL";
		int num = 0;
		Statement st = null;
		ResultSet rs = null;
		try {
			st = db.createStatement();
			rs = db.executeQuery(st, sql);
			if (rs.next()) {
				num = rs.getInt(1);
				// 强制执行到底
				rs.next();
			}
			if (num > 0) {
				throw new EngineDataException("承销债券转换系数存在为空的数据(" + num + ")");
			}
		} catch (SQLException e) {
			throw new EngineSQLException("结果集获取市场风险标准法风险暴露临时表数据库异常", e);
		} finally {
			db.closeResultSet(rs);
			db.closeStatement(st);
		}
	}
	
	/**
	 * 债券特定风险计提比率校验，若有空值则抛出数据异常
	 * @param db 数据库操作对象
	 * @throws EngineSQLException 数据库操作异常
	 * @throws EngineDataException 数据异常
	 */
	private void checkDataBySRPR(DBConnection db) throws EngineSQLException, EngineDataException {
		String sql = "SELECT COUNT(1) FROM RWA_ETC_MarketExposureSTD " +
				"Where MarketRiskType = '01' And InteRateRiskType = '01' AND SRPR IS NULL";
		int num = 0;
		Statement st = null;
		ResultSet rs = null;
		try {
			st = db.createStatement();
			rs = db.executeQuery(st, sql);
			if (rs.next()) {
				num = rs.getInt(1);
				// 强制执行到底
				rs.next();
			}
			if (num > 0) {
				// throw new EngineDataException("债券特定风险计提比率存在为空的数据(" + num + ")");
				log.info("债券特定风险计提比率存在为空的数据(" + num + "),将债券类型默认为其他，计提比率为8%(一般公司计算)");
			}
			db.executeUpdate("update RWA_ETC_MarketExposureSTD set SRPR = 0.08,IRRSRType = '09' " +
					" Where MarketRiskType = '01' And InteRateRiskType = '01' AND SRPR IS NULL");
		} catch (SQLException e) {
			throw new EngineSQLException("结果集获取市场风险标准法风险暴露临时表数据库异常", e);
		} finally {
			db.closeResultSet(rs);
			db.closeStatement(st);
		}
	}
	
	/**
	 * 到期日 时段、时区、权重、收益率变化校验，若有空值则抛出数据异常
	 * @param db 数据库操作对象
	 * @throws EngineSQLException 数据库操作异常
	 * @throws EngineDataException 数据异常
	 */
	private void checkDataByMaturity(DBConnection db) throws EngineSQLException, EngineDataException {
		String sql = "SELECT COUNT(1) FROM RWA_ETC_MarketExposureSTD " +
				"Where MarketRiskType = '01' AND (MaturityTimeBand IS NULL " +
				"OR MaturityTimeBandType IS NULL OR MaturityTimeZone IS NULL " +
				"OR MaturityRW IS NULL OR MaturityYC IS NULL)";
		int num = 0;
		Statement st = null;
		ResultSet rs = null;
		try {
			st = db.createStatement();
			rs = db.executeQuery(st, sql);
			if (rs.next()) {
				num = rs.getInt(1);
				// 强制执行到底
				rs.next();
			}
			if (num > 0) {
				// throw new EngineDataException("利率风险到期日 时段、时区、权重、收益率变化存在为空的数据(" + num + ")");
				log.info("利率风险到期日 时段、时区、权重、收益率变化存在为空的数据(" + num + ")");
				db.executeUpdate("delete FROM RWA_ETC_MarketExposureSTD " +
						"Where MarketRiskType = '01' AND (MaturityTimeBand IS NULL " +
						"OR MaturityTimeBandType IS NULL OR MaturityTimeZone IS NULL " +
						"OR MaturityRW IS NULL OR MaturityYC IS NULL)");
				log.info("删除异常数据成功");
			}
		} catch (SQLException e) {
			throw new EngineSQLException("结果集获取市场风险标准法风险暴露临时表数据库异常", e);
		} finally {
			db.closeResultSet(rs);
			db.closeStatement(st);
		}
	}
	
	/**
	 * 久期法 时段、时区、收益率变化校验，若有空值则抛出数据异常
	 * @param db 数据库操作对象
	 * @throws EngineSQLException 数据库操作异常
	 * @throws EngineDataException 数据异常
	 */
	private void checkDataByDuration(DBConnection db) throws EngineSQLException, EngineDataException {
		String sql = "SELECT COUNT(1) FROM RWA_ETC_MarketExposureSTD " +
				"Where MarketRiskType = '01' AND (DurationTimeBand IS NULL " +
				"OR DurationTimeBandType IS NULL OR DurationTimeZone IS NULL OR DurationYC IS NULL)";
		int num = 0;
		Statement st = null;
		ResultSet rs = null;
		try {
			st = db.createStatement();
			rs = db.executeQuery(st, sql);
			if (rs.next()) {
				num = rs.getInt(1);
				// 强制执行到底
				rs.next();
			}
			if (num > 0) {
				throw new EngineDataException("利率风险久期法 时段、时区、收益率变化存在为空的数据(" + num + ")");
			}
		} catch (SQLException e) {
			throw new EngineSQLException("结果集获取市场风险标准法风险暴露临时表数据库异常", e);
		} finally {
			db.closeResultSet(rs);
			db.closeStatement(st);
		}
	}
	
}
