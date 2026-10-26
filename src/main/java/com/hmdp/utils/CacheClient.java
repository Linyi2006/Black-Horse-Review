package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.CACHE_NULL_TTL;
import static com.hmdp.utils.RedisConstants.LOCK_SHOP_TTL;

@Component
@Slf4j
public class CacheClient {

    /**
     * 缓存重建线程池
     */
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    private final StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit) {
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    public <R, ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback,
                                          Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        String json = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(json)) {
            return JSONUtil.toBean(json, type);
        }
        if (json != null) {
            return null;
        }
        R r = dbFallback.apply(id);
        if (r == null) {
            // 缓存空值，防止缓存穿透
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        this.set(key, r, time, unit);
        return r;
    }

    public <R, ID> R queryWithLogicalExpire(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback,
                                            Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        // 1. 从 Redis 查询缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2. 未命中，直接返回（逻辑过期方案只适合提前预热好的热点数据）
        if (StrUtil.isBlank(json)) {
            return null;
        }
        // 3. 命中，解析出数据和逻辑过期时间
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        if (redisData.getExpireTime() == null || redisData.getData() == null) {
            // 兼容普通缓存格式，删除后按未命中处理
            stringRedisTemplate.delete(key);
            return null;
        }
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        // 4. 未过期，直接返回
        if (redisData.getExpireTime().isAfter(LocalDateTime.now())) {
            return r;
        }
        // 5. 已过期，尝试获取互斥锁，只放一个线程异步重建缓存
        String lockKey = getLockKey(key);
        if (tryLock(lockKey)) {
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    R dbData = dbFallback.apply(id);
                    if (dbData == null) {
                        stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                    } else {
                        setWithLogicalExpire(key, dbData, time, unit);
                    }
                } catch (Exception e) {
                    log.error("缓存重建失败，key={}", key, e);
                } finally {
                    unLock(lockKey);
                }
            });
        }
        // 6. 没拿到锁也返回旧数据，保证高并发下不阻塞
        return r;
    }

    private String getLockKey(String key) {
        int index = key.indexOf(':');
        return index < 0 ? "lock:" + key : "lock:" + key.substring(index + 1);
    }

    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unLock(String key) {
        stringRedisTemplate.delete(key);
    }
}
