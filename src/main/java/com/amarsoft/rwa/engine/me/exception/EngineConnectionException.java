package com.amarsoft.rwa.engine.me.exception;

/**
 * 数据库连接异常类
 * 
 * @author 陈庆
 * @version 1.0 2013-06-05
 * 
 */
public class EngineConnectionException extends Exception {
	/**
	 * UID
	 */
	private static final long serialVersionUID = 5335304145299941388L;

	/**
	 * 构造函数
	 * @param detailMessage 异常描述
	 */
	public EngineConnectionException(String detailMessage) {
		super(detailMessage);
	}

	/**
	 * 构造函数
	 * @param cause 异常对象
	 */
	public EngineConnectionException(Throwable cause) {
		super(cause);
	}

	/**
	 * 构造函数
	 * @param reason 异常原因
	 * @param cause 异常对象
	 */
	public EngineConnectionException(String reason, Throwable cause) {
		super(reason, cause);
	}

	/*
	 * 重写toString方法
	 * @see java.lang.Throwable#toString()
	 */
	@Override
	public String toString() {
		return "数据库连接异常：" + this.getMessage();
	}

}
