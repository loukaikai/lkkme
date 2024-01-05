package com.amarsoft.rwa.engine.service;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import com.amarsoft.rwa.engine.exception.ParamConfigException;
import com.amarsoft.rwa.engine.util.DataUtils;
import com.amarsoft.rwa.engine.util.JsonUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RKeys;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @description:
 * @author: chenqing
 * @create: 2023/8/15 23:55
 **/
@Service
@Slf4j
public class CacheService {

    @Autowired
    private RedissonClient redissonClient;

    /**
     * TODO 需要调整为 三层 ID 缓存， 两层 不够用
     */
    public static Map<String, Map<String, String>> cacheMap = new ConcurrentHashMap<>();

    public static String CACHE_PREFIX = "CACHE";

//    public String getCache(String name, String key) {
//        Map<String, String> cache = cacheMap.get(name);
//        if (cache == null) {
//            return null;
//        }
//        return cache.get(key);
//    }

//    public void putCache(String name, String key, Object value) {
//        Map<String, String> cache = cacheMap.get(name);
//        if (cache == null) {
//            cache = new ConcurrentHashMap<>();
//            cacheMap.put(name, cache);
//        }
//        cache.put(key, JsonUtils.object2Json(value));
//    }

//    public void removeCache(String name, String key) {
//        Map<String, String> cache = cacheMap.get(name);
//        if (cache != null) {
//            cache.remove(key);
//        }
//    }

    public List<String> findKeyList(String pattern) {
        Iterable<String> keys = redissonClient.getKeys().getKeysByPattern(pattern + "*");
        List<String> list = new ArrayList<>();
        if (CollUtil.isEmpty(keys)) {
            return list;
        }
        for (String key : keys) {
            list.add(key);
        }
        return list;
    }

    public long deleteKeys(String pattern) {
        RKeys keys = redissonClient.getKeys();
        return keys.deleteByPattern(pattern + "*");
    }

    private String getCacheKey(String name, String mainKey) {
        // 大类主键 可默认为 0
        if (StrUtil.isEmpty(mainKey)) {
            mainKey = "0";
        }
        return DataUtils.generateKey(getCacheKey(name), mainKey);
    }

    private String getCacheKey(String name) {
        if (StrUtil.isEmpty(name)) {
            throw new ParamConfigException("缓存ID不能为空");
        }
        return DataUtils.generateKey(CACHE_PREFIX, name);
    }

    /**
     * 根据 缓存key 获取 主键key
     * @param cacheKey
     * @return
     */
    public String getMainKey(String cacheKey) {
        String[] keys = cacheKey.split(":");
        return keys[keys.length - 2];
    }

    @Retryable(value = Exception.class,maxAttempts = 3,backoff = @Backoff(delay = 1000,multiplier = 1.5))
    public List<String> getCacheKeyList(String name, String mainKey) {
        return this.findKeyList(this.getCacheKey(name, mainKey));
    }

    @Retryable(value = Exception.class,maxAttempts = 3,backoff = @Backoff(delay = 1000,multiplier = 1.5))
    public List<String> getCacheKeyList(String name) {
        return this.findKeyList(this.getCacheKey(name));
    }

    public String getCache(String name, String mainKey, String subKey) {
        Map<String, String> map = redissonClient.getMap(this.getCacheKey(name, mainKey));
        if (StrUtil.isEmpty(subKey)) {
            subKey = "0";
        }
        return map.get(subKey);
    }

    public String getCache(String name, String key) {
        return this.getCache(name, key, "0");
    }

    public String getCache(String name) {
        return this.getCache(name, "0");
    }

    @Retryable(value = Exception.class,maxAttempts = 3,backoff = @Backoff(delay = 1000,multiplier = 1.5))
    public void putCache(String name, String mainKey, String subKey, Object value) {
        Map<String, String> cache = redissonClient.getMap(this.getCacheKey(name, mainKey));
        if (StrUtil.isEmpty(subKey)) {
            subKey = "0";
        }
        cache.put(subKey, JsonUtils.object2Json(value));
    }

    @Retryable(value = Exception.class,maxAttempts = 3,backoff = @Backoff(delay = 1000,multiplier = 1.5))
    public void putCache(String name, String mainKey, Object value) {
        Map<String, String> cache = redissonClient.getMap(this.getCacheKey(name, mainKey));
        cache.put("0", JsonUtils.object2Json(value));
    }

    @Retryable(value = Exception.class,maxAttempts = 3,backoff = @Backoff(delay = 1000,multiplier = 1.5))
    public <T> void putCache(String name, String mainKey, Map<String, T> map) {
        Map<String, String> cache = redissonClient.getMap(this.getCacheKey(name, mainKey));
        for (String key : map.keySet()) {
            cache.put(key, JsonUtils.object2Json(map.get(key)));
        }
    }

    @Retryable(value = Exception.class,maxAttempts = 3,backoff = @Backoff(delay = 1000,multiplier = 1.5))
    public void putCache(String name, Object value) {
        Map<String, String> cache = redissonClient.getMap(this.getCacheKey(name, "0"));
        cache.put("0", JsonUtils.object2Json(value));
    }

    @Retryable(value = Exception.class,maxAttempts = 3,backoff = @Backoff(delay = 1000,multiplier = 1.5))
    public void removeCache(String name, String mainKey, String subKey) {
        Map<String, String> cache = redissonClient.getMap(this.getCacheKey(name, mainKey));
        if (StrUtil.isEmpty(subKey)) {
            subKey = "0";
        }
        cache.remove(subKey);
    }

    @Retryable(value = Exception.class,maxAttempts = 3,backoff = @Backoff(delay = 1000,multiplier = 1.5))
    public void deleteCache(String name, String mainKey) {
        RKeys keys = redissonClient.getKeys();
        keys.deleteByPattern(this.getCacheKey(name, mainKey));
    }

    @Retryable(value = Exception.class,maxAttempts = 3,backoff = @Backoff(delay = 1000,multiplier = 1.5))
    public long clearCache(String name) {
        return this.deleteKeys(this.getCacheKey(name));
    }

    @Retryable(value = Exception.class,maxAttempts = 3,backoff = @Backoff(delay = 1000,multiplier = 1.5))
    public <T> T getCache(String name, String mainKey, String subKey, Class<T> clazz) {
        String json = this.getCache(name, mainKey, subKey);
        if (StrUtil.isEmpty(json)) {
            return null;
        }
        return JsonUtils.json2Object(json, clazz);
    }

    @Retryable(value = Exception.class,maxAttempts = 3,backoff = @Backoff(delay = 1000,multiplier = 1.5))
    public <T> T getCache(String name, String mainKey, Class<T> clazz) {
        String json = this.getCache(name, mainKey);
        if (StrUtil.isEmpty(json)) {
            return null;
        }
        return JsonUtils.json2Object(json, clazz);
    }

    @Retryable(value = Exception.class,maxAttempts = 3,backoff = @Backoff(delay = 1000,multiplier = 1.5))
    public <T> Map<String, T> getCacheMap(String name, String mainKey, Class<T> clazz) {
        Map<String, String> cache = redissonClient.getMap(this.getCacheKey(name, mainKey));
        Map<String, T> map = new HashMap<>();
        if (CollUtil.isEmpty(cache)) {
            return map;
        }
        for (String key : cache.keySet()) {
            map.put(key, JsonUtils.json2Object(cache.get(key), clazz));
        }
        return map;
    }

    @Retryable(value = Exception.class,maxAttempts = 3,backoff = @Backoff(delay = 1000,multiplier = 1.5))
    public <T> T getCache(String name, Class<T> clazz) {
        String json = this.getCache(name);
        if (StrUtil.isEmpty(json)) {
            return null;
        }
        return JsonUtils.json2Object(json, clazz);
    }

    @Retryable(value = Exception.class,maxAttempts = 3,backoff = @Backoff(delay = 1000,multiplier = 1.5))
    public <T> T getCache(String name, String mainKey, String subKey, TypeReference<T> clazz) {
        String json = this.getCache(name, mainKey, subKey);
        if (StrUtil.isEmpty(json)) {
            return null;
        }
        return JsonUtils.json2Object(json, clazz);
    }

    @Retryable(value = Exception.class,maxAttempts = 3,backoff = @Backoff(delay = 1000,multiplier = 1.5))
    public <T> T getCache(String name, String mainKey, TypeReference<T> clazz) {
        String json = this.getCache(name, mainKey);
        if (StrUtil.isEmpty(json)) {
            return null;
        }
        return JsonUtils.json2Object(json, clazz);
    }

    @Retryable(value = Exception.class,maxAttempts = 3,backoff = @Backoff(delay = 1000,multiplier = 1.5))
    public <T> T getCache(String name, TypeReference<T> clazz) {
        String json = this.getCache(name);
        if (StrUtil.isEmpty(json)) {
            return null;
        }
        return JsonUtils.json2Object(json, clazz);
    }

}
