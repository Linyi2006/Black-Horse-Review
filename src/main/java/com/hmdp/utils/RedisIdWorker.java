package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Component
public class RedisIdWorker {
    private static final Long BEGIN_TIMESTAMP =1640995200L;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    public Long nextId(String keyPrefix){
        LocalDateTime now = LocalDateTime.now();
        Long nowSecond = now.toEpochSecond(ZoneOffset.UTC);
        Long timestamp = nowSecond - BEGIN_TIMESTAMP;
        String date=now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        Long count = stringRedisTemplate.opsForValue().increment("icr" + keyPrefix + ":"+date);
        return timestamp << 32 | count;
    }
}
