package com.amarsoft.rwa.engine.service;

import com.amarsoft.rwa.engine.constant.LockType;
import com.amarsoft.rwa.engine.util.DataUtils;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;

/**
 * @description:
 * @author: chenqing
 * @create: 2023/7/10 11:47
 **/
@Service
@Slf4j
public class LockService {

    @Autowired
    private RedissonClient redissonClient;

    public static Map<String, Lock> lockMap = new ConcurrentHashMap<>();

    public static String LOCK_PREFIX = "LOCK";

//    public Lock getLock(LockType type, String ... ids) {
//        String key = this.getLockKey(type,ids);
//        Lock lock = lockMap.get(key);
//        if (lock == null) {
//            lock = new ReentrantLock();
//            lockMap.put(key, lock);
//        }
//        return lock;
//    }

    public String getLockKey(LockType type, String ... ids) {
        return DataUtils.generateKey(LOCK_PREFIX, type.getCode(), DataUtils.generateKey(ids));
    }

    @Retryable(value = Exception.class,maxAttempts = 3,backoff = @Backoff(delay = 1000,multiplier = 1.5))
    public Lock getLock(LockType type, String ... ids) {
        // 存在 连接 不稳定的情况
        String key = this.getLockKey(type,ids);
        return redissonClient.getLock(key);
    }

    @Retryable(value = Exception.class,maxAttempts = 3,backoff = @Backoff(delay = 1000,multiplier = 1.5))
    public Lock getCallApiLock(String... ids) {
        return this.getLock(LockType.EXE, ids);
    }

    @Retryable(value = Exception.class,maxAttempts = 3,backoff = @Backoff(delay = 1000,multiplier = 1.5))
    public Lock getCallApiLock(LockType type, String... ids) {
        return this.getLock(type, ids);
    }

    @Retryable(value = Exception.class,maxAttempts = 3,backoff = @Backoff(delay = 1000,multiplier = 1.5))
    public Lock getProcLock() {
        return this.getLock(LockType.PROC, "0");
    }

    @Retryable(value = Exception.class,maxAttempts = 3,backoff = @Backoff(delay = 1000,multiplier = 1.5))
    public Lock getCacheLock(String... ids) {
        return this.getLock(LockType.CACHE, ids);
    }

}
