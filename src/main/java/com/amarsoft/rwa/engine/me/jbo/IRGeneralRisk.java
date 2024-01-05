package com.amarsoft.rwa.engine.me.jbo;

/**
 * 利率一般风险结果
 * @author 陈庆
 * @version 1.0 2015-09-08
 *
 */
public class IRGeneralRisk {
	
	/** 币种 */
	private String currency;
	/** 垂直资本要求总额 */
	private double gvcr;
	/** 第一区横向资本要求 */
	private double hcr11;
	/** 第二区横向资本要求 */
	private double hcr22;
	/** 第三区横向资本要求 */
	private double hcr33;
	/** 第一区及第二区横向资本要求 */
	private double hcr12;
	/** 第二区及第三区横向资本要求 */
	private double hcr23;
	/** 第一区及第三区横向资本要求 */
	private double hcr13;
	/** 横向资本要求总额 */
	private double ghcr;
	/** 交易账户加权净头寸 */
	private double wnp;
	/** 交易账户资本要求 */
	private double tbcr;
	/** 一般风险资本要求 */
	private double grcr;
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "[利率一般风险结果(币种：" + currency + ")]";
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
	 * 获取垂直资本要求总额
	 * @return gvcr 垂直资本要求总额
	 */
	public double getGvcr() {
		return gvcr;
	}

	/**
	 * 设置垂直资本要求总额
	 * @param gvcr 垂直资本要求总额
	 */
	public void setGvcr(double gvcr) {
		this.gvcr = gvcr;
	}

	/**
	 * 获取第一区横向资本要求
	 * @return hcr11 第一区横向资本要求
	 */
	public double getHcr11() {
		return hcr11;
	}

	/**
	 * 设置第一区横向资本要求
	 * @param hcr11 第一区横向资本要求
	 */
	public void setHcr11(double hcr11) {
		this.hcr11 = hcr11;
	}

	/**
	 * 获取第二区横向资本要求
	 * @return hcr22 第二区横向资本要求
	 */
	public double getHcr22() {
		return hcr22;
	}

	/**
	 * 设置第二区横向资本要求
	 * @param hcr22 第二区横向资本要求
	 */
	public void setHcr22(double hcr22) {
		this.hcr22 = hcr22;
	}

	/**
	 * 获取第三区横向资本要求
	 * @return hcr33 第三区横向资本要求
	 */
	public double getHcr33() {
		return hcr33;
	}

	/**
	 * 设置第三区横向资本要求
	 * @param hcr33 第三区横向资本要求
	 */
	public void setHcr33(double hcr33) {
		this.hcr33 = hcr33;
	}

	/**
	 * 获取第一区及第二区横向资本要求
	 * @return hcr12 第一区及第二区横向资本要求
	 */
	public double getHcr12() {
		return hcr12;
	}

	/**
	 * 设置第一区及第二区横向资本要求
	 * @param hcr12 第一区及第二区横向资本要求
	 */
	public void setHcr12(double hcr12) {
		this.hcr12 = hcr12;
	}

	/**
	 * 获取第二区及第三区横向资本要求
	 * @return hcr23 第二区及第三区横向资本要求
	 */
	public double getHcr23() {
		return hcr23;
	}

	/**
	 * 设置第二区及第三区横向资本要求
	 * @param hcr23 第二区及第三区横向资本要求
	 */
	public void setHcr23(double hcr23) {
		this.hcr23 = hcr23;
	}

	/**
	 * 获取第一区及第三区横向资本要求
	 * @return hcr13 第一区及第三区横向资本要求
	 */
	public double getHcr13() {
		return hcr13;
	}

	/**
	 * 设置第一区及第三区横向资本要求
	 * @param hcr13 第一区及第三区横向资本要求
	 */
	public void setHcr13(double hcr13) {
		this.hcr13 = hcr13;
	}

	/**
	 * 获取横向资本要求总额
	 * @return ghcr 横向资本要求总额
	 */
	public double getGhcr() {
		return ghcr;
	}

	/**
	 * 设置横向资本要求总额
	 * @param ghcr 横向资本要求总额
	 */
	public void setGhcr(double ghcr) {
		this.ghcr = ghcr;
	}

	/**
	 * 获取交易账户加权净头寸
	 * @return wnp 交易账户加权净头寸
	 */
	public double getWnp() {
		return wnp;
	}

	/**
	 * 设置交易账户加权净头寸
	 * @param wnp 交易账户加权净头寸
	 */
	public void setWnp(double wnp) {
		this.wnp = wnp;
	}

	/**
	 * 获取交易账户资本要求
	 * @return tbcr 交易账户资本要求
	 */
	public double getTbcr() {
		return tbcr;
	}

	/**
	 * 设置交易账户资本要求
	 * @param tbcr 交易账户资本要求
	 */
	public void setTbcr(double tbcr) {
		this.tbcr = tbcr;
	}

	/**
	 * 获取一般风险资本要求
	 * @return grcr 一般风险资本要求
	 */
	public double getGrcr() {
		return grcr;
	}

	/**
	 * 设置一般风险资本要求
	 * @param grcr 一般风险资本要求
	 */
	public void setGrcr(double grcr) {
		this.grcr = grcr;
	}
	
}
