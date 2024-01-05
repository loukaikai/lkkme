/**
 * Copyright @2013 CIB Co. Ltd.
 * All right reserved
 */
package com.amarsoft.rwa.engine.me.exception;

/**
 * 非法数据异常类
 * 
 * @author 陈庆
 * @version 1.0 2013-06-06
 * 
 */
public class EngineDataException extends Exception {
	/**
	 * UID
	 */
	private static final long serialVersionUID = -4615693721758130578L;
	
	/**
	 * 构造函数
	 * @param detailMessage 异常描述
	 */
	public EngineDataException(String detailMessage) {
		super(detailMessage);
	}

	/**
	 * 构造函数
	 * @param cause 异常对象
	 */
	public EngineDataException(Throwable cause) {
		super(cause);
	}

	/**
	 * 构造函数
	 * @param reason 异常原因
	 * @param cause 异常对象
	 */
	public EngineDataException(String reason, Throwable cause) {
		super(reason, cause);
	}

	/*
	 * 重写toString方法
	 * @see java.lang.Throwable#toString()
	 */
	@Override
	public String toString() {
		return "非法数据异常：" + this.getMessage();
	}


}
