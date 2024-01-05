package com.amarsoft.rwa.engine.config;

import com.amarsoft.rwa.engine.util.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.MethodParameter;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.mvc.method.annotation.RequestBodyAdvice;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;

@ControllerAdvice
@Slf4j
public class LogRequestBodyAdvice implements RequestBodyAdvice {

    @Override
    public boolean supports(MethodParameter methodParameter, Type targetType,
                            Class<? extends HttpMessageConverter<?>> converterType) {
        return true;
    }

    @Override
    public Object handleEmptyBody(Object body, HttpInputMessage inputMessage,
                                  MethodParameter parameter, Type targetType,
                                  Class<? extends HttpMessageConverter<?>> converterType) {
        return body;
    }

    @Override
    public HttpInputMessage beforeBodyRead(HttpInputMessage inputMessage,
                                           MethodParameter parameter, Type targetType,
                                           Class<? extends HttpMessageConverter<?>> converterType) throws IOException {
        return inputMessage;
    }

    @Override
    public Object afterBodyRead(Object body, HttpInputMessage inputMessage,
                                MethodParameter parameter, Type targetType,
                                Class<? extends HttpMessageConverter<?>> converterType) {
        Method method = parameter.getMethod();
        String classMappingUri = getClassMappingUri(method.getDeclaringClass());
        if (classMappingUri.endsWith("/")) {
            classMappingUri = classMappingUri.substring(0, classMappingUri.length()-1);
        }
        String methodMappingUri = getMethodMappingUri(method);
        if (!methodMappingUri.startsWith("/")) {
            methodMappingUri = "/" + methodMappingUri;
        }
        log.debug("uri={} | requestBody={}", classMappingUri + methodMappingUri, JsonUtils.object2Json(body));
        return body;
    }

    private String getMethodMappingUri(Method method) {
        RequestMapping methodDeclaredAnnotation = method.getDeclaredAnnotation(RequestMapping.class);
        return methodDeclaredAnnotation == null ? "" : getMaxLength(methodDeclaredAnnotation.value());
    }

    private String getClassMappingUri(Class<?> declaringClass) {
        RequestMapping classDeclaredAnnotation = declaringClass.getDeclaredAnnotation(RequestMapping.class);
        return classDeclaredAnnotation == null ? "" : getMaxLength(classDeclaredAnnotation.value());
    }

    private String getMaxLength(String[] strings) {
        String methodMappingUri = "";
        for (String string : strings) {
            if (string.length() > methodMappingUri.length()) {
                methodMappingUri = string;
            }
        }
        return methodMappingUri;
    }

}
