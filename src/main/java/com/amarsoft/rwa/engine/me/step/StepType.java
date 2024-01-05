package com.amarsoft.rwa.engine.me.step;

/**
 * 步骤枚举类型
 * 
 * @author 陈庆
 * @version 1.0 2013-06-06
 * 
 */
public enum StepType {

	LOADDATA("0201", "数据加载"), 
	MAPPINGPARAMS("0202", "参数映射"), 
	CALCULATERWA("0203", "RWA计算"), 
	INSERTRESULT("0204", "结果写入");
	
	/** 步骤编号 */
	private String id;
	
	/** 步骤名称 */
	private String name;
	
	/**
	 * 构造方法
	 * @param id 步骤编号
	 * @param name 步骤名称
	 */
	private StepType(String id, String name) {
		this.id = id;
		this.name = name;
	}

	/* (non-Javadoc)
	 * @see java.lang.Enum#toString()
	 */
	@Override
	public String toString() {
		return "步骤[" + this.name + "]";
	}

	/**
	 * 获取步骤编号
	 * @return 步骤编号
	 */
	public String getId() {
		return id;
	}

	/**
	 * 设置步骤编号
	 * @param id 步骤编号
	 */
	public void setId(String id) {
		this.id = id;
	}

	/**
	 * 获取步骤名称
	 * @return 步骤名称
	 */
	public String getName() {
		return name;
	}

	/**
	 * 设置步骤名称
	 * @param name 步骤名称
	 */
	public void setName(String name) {
		this.name = name;
	}
	
}
