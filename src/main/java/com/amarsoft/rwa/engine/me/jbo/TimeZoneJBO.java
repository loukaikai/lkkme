package com.amarsoft.rwa.engine.me.jbo;

/**
 * 时区计算类
 * @author 陈庆
 * @version 1.0 2015-09-08
 *
 */
public class TimeZoneJBO {
	
	/** 币种 */
	private String currency;
	/** 时区1 */
	private String timeZone1;
	/** 时区2 */
	private String timeZone2;
	/** 垂直资本要求 */
	private double vcr;
	/** 加权多头头寸 */
	private double wlp;
	/** 加权空头头寸 */
	private double wsp;
	/** 加权对冲头寸 */
	private double whp;
	/** 加权净头寸 */
	private double wnp;
	/** 风险权重 */
	private double rw;
	/** 横向资本要求 */
	private double hcr;
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "[利率风险时区结果(币种：" + currency + ")(时区1：" + timeZone1 + ")(时区2：" + timeZone2 + ")]";
	}

	/**
	 * 获取币种
	 * @return currency 币种
	 */
	public String getCurrency() {
		return currency;
	}

	/**
	 * 设置币种
	 * @param currency 币种
	 */
	public void setCurrency(String currency) {
		this.currency = currency;
	}

	/**
	 * 获取时区1
	 * @return timeZone1 时区1
	 */
	public String getTimeZone1() {
		return timeZone1;
	}

	/**
	 * 设置时区1
	 * @param timeZone1 时区1
	 */
	public void setTimeZone1(String timeZone1) {
		this.timeZone1 = timeZone1;
	}

	/**
	 * 获取时区2
	 * @return timeZone2 时区2
	 */
	public String getTimeZone2() {
		return timeZone2;
	}

	/**
	 * 设置时区2
	 * @param timeZone2 时区2
	 */
	public void setTimeZone2(String timeZone2) {
		this.timeZone2 = timeZone2;
	}

	/**
	 * 获取垂直资本要求
	 * @return vcr 垂直资本要求
	 */
	public double getVcr() {
		return vcr;
	}

	/**
	 * 设置垂直资本要求
	 * @param vcr 垂直资本要求
	 */
	public void setVcr(double vcr) {
		this.vcr = vcr;
	}

	/**
	 * 获取加权多头头寸
	 * @return wlp 加权多头头寸
	 */
	public double getWlp() {
		return wlp;
	}

	/**
	 * 设置加权多头头寸
	 * @param wlp 加权多头头寸
	 */
	public void setWlp(double wlp) {
		this.wlp = wlp;
	}

	/**
	 * 获取加权空头头寸
	 * @return wsp 加权空头头寸
	 */
	public double getWsp() {
		return wsp;
	}

	/**
	 * 设置加权空头头寸
	 * @param wsp 加权空头头寸
	 */
	public void setWsp(double wsp) {
		this.wsp = wsp;
	}

	/**
	 * 获取加权对冲头寸
	 * @return whp 加权对冲头寸
	 */
	public double getWhp() {
		return whp;
	}

	/**
	 * 设置加权对冲头寸
	 * @param whp 加权对冲头寸
	 */
	public void setWhp(double whp) {
		this.whp = whp;
	}

	/**
	 * 获取加权净头寸
	 * @return wnp 加权净头寸
	 */
	public double getWnp() {
		return wnp;
	}

	/**
	 * 设置加权净头寸
	 * @param wnp 加权净头寸
	 */
	public void setWnp(double wnp) {
		this.wnp = wnp;
	}

	/**
	 * 获取风险权重
	 * @return rw 风险权重
	 */
	public double getRw() {
		return rw;
	}

	/**
	 * 设置风险权重
	 * @param rw 风险权重
	 */
	public void setRw(double rw) {
		this.rw = rw;
	}

	/**
	 * 获取横向资本要求
	 * @return hcr 横向资本要求
	 */
	public double getHcr() {
		return hcr;
	}

	/**
	 * 设置横向资本要求
	 * @param hcr 横向资本要求
	 */
	public void setHcr(double hcr) {
		this.hcr = hcr;
	}

}
