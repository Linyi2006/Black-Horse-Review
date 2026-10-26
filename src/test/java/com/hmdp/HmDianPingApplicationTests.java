package com.hmdp;

import com.hmdp.service.IShopService;
import com.hmdp.utils.CacheClient;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;

@SpringBootTest
class HmDianPingApplicationTests {

    @Resource
    private IShopService shopService;

    @Resource
    private CacheClient cacheClient;

    /**
     * 预热店铺缓存（逻辑过期时间 10 秒，方便观察缓存重建）
     */
    @Test
    void testSaveShop2Redis() {
        cacheClient.setWithLogicalExpire(CACHE_SHOP_KEY + 1L, shopService.getById(1L), 10L, TimeUnit.SECONDS);
    }
}
