package com.hmdp;

import com.hmdp.entity.Shop;
import com.hmdp.service.IShopService;
import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisIdWork;
import com.hmdp.utils.RegexUtils;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@SpringBootTest
class HmDianPingApplicationTests {

    @Autowired
    private ShopServiceImpl shopService;
    @Resource
    private RedisIdWork redisIdWordk;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Test
    void loadShopData() {
//        1.查询店铺信息
        List<Shop> list = shopService.list();
//        2.把店铺分组，按照typeId分组，typeId
        Map<Long, List<Shop>> map = list.stream().collect(Collectors.groupingBy(Shop::getTypeId));
//        3.分批写入redis
        for (Map.Entry<Long, List<Shop>> longListEntry : map.entrySet()) {
//            3.1 获取类型id
            Long typeId = longListEntry.getKey();
            String key = RedisConstants.SHOP_GEO_KEY + typeId;
//            3.2 获取同类型的店铺集合
            List<Shop> shopList = longListEntry.getValue();
            List<RedisGeoCommands.GeoLocation<String>> locations = new ArrayList<>(shopList.size());
            // 3.3.写入redis GEOADD key 经度 纬度 member
            for (Shop shop : shopList) {
               locations.add(new RedisGeoCommands.GeoLocation<>(shop.getId().toString(),new Point(shop.getX(),shop.getY())));
            }
            stringRedisTemplate.opsForGeo().add(key,locations);
        }
    }

    @Test
    void testPhone(){
        System.out.println(RegexUtils.isPhoneInvalid("13812345678"));
    }

//    @Test
//    void testSaveShop() throws InterruptedException {
//        shopService.saveShop2Redis(1L, 10L);
//    }

    private static final ExecutorService es = Executors.newFixedThreadPool(500);
    @Test
    void testIdWork() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(300);
        Runnable r = () -> {
            for (int i = 0; i < 100; i++) {
                long order = redisIdWordk.nextId("order");
                System.out.println("id="+order);
            }
            latch.countDown();
        };
        long begin = System.currentTimeMillis();
        for (int i = 0; i < 300; i++) {
            es.submit(r);
        }
        latch.await();
        long end = System.currentTimeMillis();
        System.out.println(end-begin);
    }
    @Value("http://${spring.redis.host}:${spring.redis.port}")
    private String host;

    @Test
    void test1(){
        System.out.printf(host);
    }


}
