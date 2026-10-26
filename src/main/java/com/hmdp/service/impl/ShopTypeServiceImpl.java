package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TYPE_KEY;
import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TYPE_TTL;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryTypeList() {
        String key = CACHE_SHOP_TYPE_KEY + "list";
        // 1. 从 Redis 查询缓存
        String typeListJson = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(typeListJson)) {
            // 2. 命中，反序列化后直接返回
            List<ShopType> typeList = JSONUtil.toList(typeListJson, ShopType.class);
            return Result.ok(typeList);
        }
        // 3. 未命中，查询数据库
        List<ShopType> typeList = query().orderByAsc("sort").list();
        if (typeList == null || typeList.isEmpty()) {
            return Result.fail("店铺类型不存在");
        }
        // 4. 写入 Redis 并设置过期时间
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(typeList), CACHE_SHOP_TYPE_TTL, TimeUnit.MINUTES);
        return Result.ok(typeList);
    }
}
