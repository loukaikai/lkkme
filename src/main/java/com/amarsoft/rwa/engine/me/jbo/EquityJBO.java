package com.amarsoft.rwa.engine.me.jbo;

/**
 * 股票风险结果
 * @author 陈庆
 * @version 1.0 2015-09-08
 *
 */
public class EquityJBO {

	/** 交易地区 */
	private String exchangeArea;
	/** 多头总头寸 */
	private double glp; 
	/** 空头总头寸 */
	private double gsp; 
	/** 总头寸 */
	private double gp; 
	/** 净头寸 */
	private double np; 
	/** 特定风险计提比率 */
	private double srpr; 
	/** 一般风险计提比率 */
	private double grpr; 
	/** 特定风险资本要求 */
	private double srcr; 
	/** 一般风险资本要求 */
	private double grcr; 
	/** 资本要求 */
	private double rc;

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "[股票风险结果(交易地区：" + exchangeArea + ")]";
	}
	
	/**
	 * 获取交易地区
	 * @return exchangeArea 交易地区
	 */
	public String getExchangeArea() {
		return exchangeArea;
	}

	/**
	 * 设置交易地区
	 * @param exchangeArea 交易地区
	 */
	public void setExchangeArea(String exchangeArea) {
		this.exchangeArea = exchangeArea;
	}

	/**
	 * 获取多头总头寸
	 * @return glp 多头总头寸
	 */
	public double getGlp() {
		return glp;
	}

	/**
	 * 设置多头总头寸
	 * @param glp 多头总头寸
	 */
	public void setGlp(double glp) {
		this.glp = glp;
	}

	/**
	 * 获取空头总头寸
	 * @return gsp 空头总头寸
	 */
	public double getGsp() {
		return gsp;
	}

	/**
	 * 设置空头总头寸
	 * @param gsp 空头总头寸
	 */
	public void setGsp(double gsp) {
		this.gsp = gsp;
	}

	/**
	 * 获取总头寸
	 * @return gp 总头寸
	 */
	public double getGp() {
		return gp;
	}

	/**
	 * 设置总头寸
	 * @param gp 总头寸
	 */
	public void setGp(double gp) {
		this.gp = gp;
	}

	/**
	 * 获取净头寸
	 * @return np 净头寸
	 */
	public double getNp() {
		return np;
	}

	/**
	 * 设置净头寸
	 * @param np 净头寸
	 */
	public void setNp(double np) {
		this.np = np;
	}

	/**
	 * 获取特定风险计提比率
	 * @return srpr 特定风险计提比率
	 */
	public double getSrpr() {
		return srpr;
	}

	/**
	 * 设置特定风险计提比率
	 * @param srpr 特定风险计提比率
	 */
	public void setSrpr(double srpr) {
		this.srpr = srpr;
	}

	/**
	 * 获取一般风险计提比率
	 * @return grpr 一般风险计提比率
	 */
	public double getGrpr() {
		return grpr;
	}

	/**
	 * 设置一般风险计提比率
	 * @param grpr 一般风险计提比率
	 */
	public void setGrpr(double grpr) {
		this.grpr = grpr;
	}

	/**
	 * 获取特定风险资本要求
	 * @return srcr 特定风险资本要求
	 */
	public double getSrcr() {
		return srcr;
	}

	/**
	 * 设置特定风险资本要求
	 * @param srcr 特定风险资本要求
	 */
	public void setSrcr(double srcr) {
		this.srcr = srcr;
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

	/**
	 * 获取资本要求
	 * @return rc 资本要求
	 */
	public double getRc() {
		return rc;
	}

	/**
	 * 设置资本要求
	 * @param rc 资本要求
	 */
	public void setRc(double rc) {
		this.rc = rc;
	} 
	
}
