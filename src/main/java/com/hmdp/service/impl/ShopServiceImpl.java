package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.SystemConstants;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id){
        // 逻辑过期方案（需提前预热缓存）：
        // Shop shop = cacheClient.queryWithLogicalExpire(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_LOGICAL_TTL, TimeUnit.SECONDS);
        Shop shop = cacheClient.queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);
        if(shop == null) {
            return Result.fail("店铺不存在");
        }
        return Result.ok(shop);
    }

    public Shop queryWithMutex(Long id) {
        String key= CACHE_SHOP_KEY + id;
        String shopJson=stringRedisTemplate.opsForValue().get(key);
        if(StrUtil.isNotBlank(shopJson)){
            Shop shop = JSONUtil.toBean(shopJson,Shop.class);
            return shop;
        }
        if(shopJson!=null){
            return null;
        }
        String lockKey = LOCK_SHOP_KEY + id;
        Shop shop = null;
        try {
            boolean isLock = tryLock(lockKey);
            if (!isLock) {
                Thread.sleep(50);
                return queryWithMutex(id);
            }
            shop = getById(id);
            if (shop == null) {
                stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally{
            unLock(lockKey);
        }
        return shop;
    }
    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }
    private void unLock(String key){
        stringRedisTemplate.delete(key);
    }

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺id不能为空");
        }
        updateById(shop);
        stringRedisTemplate.delete(CACHE_SHOP_KEY + shop.getId());
        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        if(x==null||y==null){
            Page<Shop> page =query()
                    .eq("type_id",typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            return Result.ok(page.getRecords());
        }
        // 没有坐标时走上方的数据库分页，这里是需要按坐标查询附近商户
        // 1.计算分页参数：GEO 不支持 offset，所以一次取出到当前页末尾，再在内存中 skip
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;
        String key = SHOP_GEO_KEY + typeId;
        // 2.以（x，y）为圆心、5000 米为半径查询，显式指定 ASC 才能按距离由近到远排序
        // 用 radius（GEORADIUS）而不是 search（GEOSEARCH）：后者需要 Redis 6.2+，本地 Redis 为 5.0.14
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo()
                .radius(key,
                        new Circle(new Point(x, y), new Distance(5000)),
                        RedisGeoCommands.GeoRadiusCommandArgs.newGeoRadiusArgs().includeDistance()
                                .sortAscending()
                                .limit(end)
                );
        // 3.查询异常或 key 不存在时返回空
        if (results == null) {
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if (list.size() <= from) {
            // 范围内的商户已全部翻完，没有下一页
            return Result.ok(Collections.emptyList());
        }
        // 4.截取当前页数据，解析出店铺 id
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> pageTuples = list.stream()
                .skip(from)
                .limit(SystemConstants.DEFAULT_PAGE_SIZE)
                .collect(Collectors.toList());
        List<Long> ids = pageTuples.stream()
                .map(tuple -> Long.valueOf(tuple.getContent().getName()))
                .collect(Collectors.toList());
        // 5.根据店铺 id 查店铺信息，listByIds 不保证顺序，所以先转 Map
        Map<Long, Shop> shopMap = listByIds(ids).stream()
                .collect(Collectors.toMap(Shop::getId, shop -> shop));
        // 6.按距离顺序回填 distance，赋值给每个店铺对象
        List<Shop> shopList = pageTuples.stream()
                .map(tuple -> {
                    Shop shop = shopMap.get(Long.valueOf(tuple.getContent().getName()));
                    if (shop == null) {
                        // GEO 中有、数据库已删除的脏数据，直接丢弃
                        return null;
                    }
                    shop.setDistance(tuple.getDistance().getValue());
                    return shop;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        return Result.ok(shopList);
    }

    @Override
    public void cacheAllShopToGeo() {
        List<Shop> shops = query().list();
        for (Shop shop : shops) {
            Point point = new Point(shop.getX(), shop.getY());
            // member 为店铺 id，按商铺类型分组存放，便于同一类型的附近查询
            stringRedisTemplate.opsForGeo().add(SHOP_GEO_KEY + shop.getTypeId(), point, shop.getId().toString());
        }
    }
}
