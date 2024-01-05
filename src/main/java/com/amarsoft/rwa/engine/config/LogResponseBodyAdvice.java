package com.amarsoft.rwa.engine.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.core.MethodParameter;
import org.springframework.http.MediaType;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyAdvice;
import com.amarsoft.rwa.engine.util.JsonUtils;

@ControllerAdvice
@Slf4j
public class LogResponseBodyAdvice implements ResponseBodyAdvice {

    @Override
    public boolean supports(MethodParameter returnType, Class converterType) {
        return true;
    }

    @Override
    public Object beforeBodyWrite(Object body, MethodParameter returnType, MediaType selectedContentType, Class selectedConverterType, ServerHttpRequest request, ServerHttpResponse response) {
        if (body == null) {
            log.debug("uri={} | responseBody={null}", request.getURI().getPath());
            return null;
        }
        try {
            log.debug("uri={} | responseBody={}", request.getURI().getPath(), JsonUtils.object2Json(body));
        } catch (Exception e) {
            log.debug("uri={} | responseBody={}", request.getURI().getPath(), body);
        }
        return body;
    }

}
