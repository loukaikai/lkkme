package com.amarsoft.rwa.engine.me.jbo;

/**
 * 商品风险结果
 * @author 陈庆
 * @version 1.0 2015-09-08
 *
 */
public class CommodityJBO {

	/** 商品种类名称 */
	private String commodityName;
	/** 多头总头寸 */
	private double glp;
	/** 空头总头寸 */
	private double gsp;
	/** 净头寸 */
	private double np;
	/** 总头寸 */
	private double gp;
	/** 净头寸计提比率 */
	private double nppr;
	/** 总头寸计提比率 */
	private double gppr;
	/** 资本要求 */
	private double rc;
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "[商品风险结果(商品种类名称：" + commodityName + ")]";
	}

	/**
	 * 获取商品种类名称
	 * @return commodityName 商品种类名称
	 */
	public String getCommodityName() {
		return commodityName;
	}

	/**
	 * 设置商品种类名称
	 * @param commodityName 商品种类名称
	 */
	public void setCommodityName(String commodityName) {
		this.commodityName = commodityName;
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
	 * 获取净头寸计提比率
	 * @return nppr 净头寸计提比率
	 */
	public double getNppr() {
		return nppr;
	}

	/**
	 * 设置净头寸计提比率
	 * @param nppr 净头寸计提比率
	 */
	public void setNppr(double nppr) {
		this.nppr = nppr;
	}

	/**
	 * 获取总头寸计提比率
	 * @return gppr 总头寸计提比率
	 */
	public double getGppr() {
		return gppr;
	}

	/**
	 * 设置总头寸计提比率
	 * @param gppr 总头寸计提比率
	 */
	public void setGppr(double gppr) {
		this.gppr = gppr;
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
