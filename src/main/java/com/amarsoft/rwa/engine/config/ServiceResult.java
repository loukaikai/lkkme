package com.amarsoft.rwa.engine.config;

import com.amarsoft.rwa.engine.constant.StatusCodeEnum;
import lombok.Data;

import java.io.Serializable;
import java.time.LocalDateTime;

@Data
public class ServiceResult implements Serializable {

    public static final long serialVersionUID = 952707170223L;

    private int code;
    private String message;
    private LocalDateTime time;
    private Object data;

    public ServiceResult fillCode(StatusCodeEnum code) {
        return fillCode(code.getCode(), code.getMessage());
    }

    public ServiceResult fillCode(int code, String message) {
        this.setCode(code);
        this.setMessage(message);
        this.setTime(LocalDateTime.now());
        return this;
    }

    public ServiceResult fillData(Object data) {
        this.setData(data);
        return this;
    }

    private static ServiceResult result(StatusCodeEnum status) {
        ServiceResult result = new ServiceResult();
        result.fillCode(status);
        return result;
    }

    private static ServiceResult result(int code, String message) {
        ServiceResult result = new ServiceResult();
        result.fillCode(code, message);
        return result;
    }

    public static ServiceResult success() {
        return result(StatusCodeEnum.SUCCESS);
    }

    public static ServiceResult success(Object object) {
        return result(StatusCodeEnum.SUCCESS).fillData(object);
    }

    public static ServiceResult error(StatusCodeEnum status) {
        return result(status);
    }

    public static ServiceResult error(StatusCodeEnum status, String message) {
        return error(status.getCode(), message);
    }

    public static ServiceResult error(int code, String message) {
        return result(code, message);
    }

    public String simpleJson() {
        if (message == null) message = "";
        return "{\"code\":" + code + ",\"message\":\"" + message + ",\"time\":\"" + time + "\"}";
    }

}
