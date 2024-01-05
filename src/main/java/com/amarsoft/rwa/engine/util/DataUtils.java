package com.amarsoft.rwa.engine.util;

import cn.hutool.core.util.StrUtil;
import lombok.extern.slf4j.Slf4j;

/**
 * @description: 数据相关工具方法类
 * @author: chenqing
 * @create: 2021/8/26 13:18
 **/
@Slf4j
public class DataUtils {

    public static String generateKey(String ... strs) {
        if (strs.length == 0) {
            throw new RuntimeException("参数不能为空");
        }
        StringBuilder sb = new StringBuilder();
        for (String s : strs) {
            if (StrUtil.isEmpty(s)) {
                continue;
            }
            sb.append(s).append(":");
        }
        if (sb.length() > 1) {
            sb.delete(sb.length()-1, sb.length());
        }
        return sb.toString();
    }

}
