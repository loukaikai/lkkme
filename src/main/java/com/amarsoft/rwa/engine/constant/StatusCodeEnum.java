package com.amarsoft.rwa.engine.constant;

public enum StatusCodeEnum {

    SUCCESS(0,""),
    JOB_PARAM_EXCEPTION(10001,"计算参数异常"),
    UNKNOWN_EXCEPTION(99999,"未知异常");

    private int code;
    private String message;

    StatusCodeEnum(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }

}
