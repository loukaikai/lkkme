package com.amarsoft.rwa.engine.me.exception;

/**
 * 数据库操作异常类
 * 
 * @author 陈庆
 * @version 1.0 2013-06-05
 * 
 */
public class EngineSQLException extends Exception {
	/**
	 * UID
	 */
	private static final long serialVersionUID = -5722591059096056688L;
	
	/**
	 * 构造函数
	 * @param detailMessage 异常描述
	 */
	public EngineSQLException(String detailMessage) {
		super(detailMessage);
	}
	/**
	 * 构造函数
	 * @param cause 异常对象
	 */
    public EngineSQLException(Throwable cause) {
        super(cause);
    }
    
    /**
     * 构造函数
     * @param reason 异常原因
     * @param cause 异常对象
     */
    public EngineSQLException(String reason, Throwable cause) {
    	super(reason,cause);
    }
    
	
	/* 
	 * 重写toString方法
	 * @see java.lang.Throwable#toString()
	 */
	@Override
	public String toString() {
		return "数据库操作异常："+this.getMessage();
	}
}
