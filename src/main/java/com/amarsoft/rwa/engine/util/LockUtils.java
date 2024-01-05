package com.amarsoft.rwa.engine.util;

import cn.hutool.core.util.ObjectUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.locks.Lock;

@Slf4j
public class LockUtils {
    /**
     * 锁超时时不抛异常
     * @param lock
     */
    public static void unlock(Lock lock) {
        if(ObjectUtil.isNotNull(lock)){
            try {
                lock.unlock();
            } catch (Exception e) {
                log.error("关闭redis锁失败，失败原因：", e);
            }
        }
    }
}
