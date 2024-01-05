package com.amarsoft.rwa.engine.config;

import com.amarsoft.rwa.engine.exception.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Controller;
import org.springframework.validation.BindException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;

/**
 * @description: 统一异常处理
 * @author: chenqing
 * @create: 2021/9/9 19:34
 **/
@Controller
@Slf4j
@ControllerAdvice
public class AppExceptionHandler {

    @ExceptionHandler(value = ParamConfigException.class)
    @ResponseBody
    public ServiceResult paramConfigHandler(HttpServletRequest req, ParamConfigException e) throws Exception {
        log.error("uri=" + req.getRequestURI() + " | [10003][参数配置异常]", e);
        return ServiceResult.error(10003, "[参数配置异常]"+e.getMessage());
    }

    @ExceptionHandler(value = BindException.class)
    @ResponseBody
    public ServiceResult bindExceptionHandler(HttpServletRequest req, BindException e) throws Exception {
        log.error("uri=" + req.getRequestURI() + " | [10001][接口参数异常]", e);
        return ServiceResult.error(10001, "[接口参数异常]"+e.getAllErrors().get(0).getDefaultMessage());
    }

    @ExceptionHandler(value = Exception.class)
    @ResponseBody
    public ServiceResult defaultErrorHandler(HttpServletRequest req, Exception e) throws Exception {
        log.error("uri=" + req.getRequestURI() + " | [99999][未知异常]", e);
        return ServiceResult.error(99999, "系统出错了");
    }

}
