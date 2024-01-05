package com.amarsoft.rwa.engine.me.calculation;

/**
 * 计算枚举类型
 * 
 * @author 陈庆
 * @version 1.0 2013-06-06
 * 
 */
public enum RWACalculationType {

	IRR("利率风险计算"), 
	ER("股票风险计算"), 
	FER("外汇风险计算"), 
	CR("商品风险计算"), 
	OR("期权风险计算");
	
	/** 计算名称 */
	private String name;
	
	/**
	 * 构造方法
	 * @param name 计算名称
	 */
	private RWACalculationType(String name) {
		this.name = name;
	}

	/* (non-Javadoc)
	 * @see java.lang.Enum#toString()
	 */
	@Override
	public String toString() {
		return this.name;
	}

	/**
	 * 获取计算名称
	 * @return 计算名称
	 */
	public String getName() {
		return name;
	}

	/**
	 * 设置计算名称
	 * @param 计算名称
	 */
	public void setName(String name) {
		this.name = name;
	}
	
}
