package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.toolkit.StringUtils;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import io.netty.util.internal.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.yaml.snakeyaml.events.Event;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;
import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TTL;

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

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
//      防止缓存穿透
//        queryWithPassThrough(id);
        Shop shop = cacheClient.queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);

//        互斥锁解决缓存击穿
//        Shop shop = queryWithMutex(id);

//        逻辑过期解决缓存击穿
//        Shop shop = queryWithLogicalExpire(id);
        cacheClient.queryWithLogicalExpire(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);
        if (shop == null){
            return Result.fail("商户不存在");
        }
        return Result.ok(shop);
    }
    /*
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
    public Shop queryWithLogicalExpire( Long id ) {
        String key = CACHE_SHOP_KEY + id;
//        1.从redis查询缓存
        String shopRedis = stringRedisTemplate.opsForValue().get(key);
        if (StringUtils.isBlank(shopRedis)) {
            return null;
        }
//        2.命中
        RedisData redisData = JSONUtil.toBean(shopRedis, RedisData.class);
        JSONObject jsonObject = (JSONObject) redisData.getData();
        Shop shop = JSONUtil.toBean(jsonObject, Shop.class);
//        3.判断是否过期
        if (redisData.getExpireTime().isAfter(LocalDateTime.now())){
            // 5.1.未过期，直接返回店铺信息
            return shop;
        }
//        过期尝试获取锁，开启新线程完成缓存重建
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        boolean tryLock = tryLock(lockKey);
        if (tryLock){

            CACHE_REBUILD_EXECUTOR.submit(() ->{
               try {
//                   重建缓存
                   this.saveShop2Redis(id, 20L);
               }catch (Exception e){
                   throw new RuntimeException(e);
               }finally {
                   unLock(lockKey);
               }
            });
        }
        return shop;
    }
    */

/*
   public void saveShop2Redis(Long id, Long expireSeconds) throws InterruptedException {
//        查询数据库
        Shop shop = getById(id);
        Thread.sleep(200);  //便于测试
//        封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
//        写入redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id,JSONUtil.toJsonStr(redisData) );
    }
*/


   /* public Shop queryWithMutex(Long id){
        String key = CACHE_SHOP_KEY + id;
//        判断redis是否有数据，有则直接返回
        String shopRedis = stringRedisTemplate.opsForValue().get(key);
        if (StringUtils.isNotBlank(shopRedis)) {
            return JSONUtil.toBean(shopRedis, Shop.class);
        }
//        判断查到的数据是否为空值,"" 则直接返回null，防止缓存穿透
        if (shopRedis != null){
            return null;
        }
        Shop shop = null;
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        try {
//        1.设置互斥锁，防止缓存击穿
            boolean isLock = tryLock(lockKey);
//         获取失败
            if (!isLock){
                Thread.sleep(50);
                return queryWithMutex(id);
            }
            shop = this.getById(id);
//            模拟缓存重建缓存
            Thread.sleep(200);
//        没有查到用户，缓存填入空值，防止缓存穿透
            if (shop == null){
                stringRedisTemplate.opsForValue().set(key, "",RedisConstants.CACHE_NULL_TTL,TimeUnit.MINUTES);
                return null;
            }
//        写入redis
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            unLock(lockKey);
        }
        return shop;
    }
*/
   /* public Shop queryWithPassThrough(Long id){
        String key = CACHE_SHOP_KEY + id;
//        判断redis是否有数据，有则直接返回
        String shopRedis = stringRedisTemplate.opsForValue().get(key);
        if (StringUtils.isNotBlank(shopRedis)) {
          return JSONUtil.toBean(shopRedis, Shop.class);
        }
//        判断查到的数据是否为空值,"" 则直接返回null，防止缓存穿透
        if (shopRedis != null){
            return null;
        }
        Shop shop = this.getById(id);
//        没有查到用户，缓存填入空值，防止缓存穿透
        if (shop == null){
            stringRedisTemplate.opsForValue().set(key, "",RedisConstants.CACHE_NULL_TTL,TimeUnit.MINUTES);
            return null;
        }
//        写入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);

        return shop;
    }
*/
   /* private boolean tryLock(String key){
        Boolean isLock = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", RedisConstants.LOCK_SHOP_TTL, TimeUnit.SECONDS);
//        防止Boolean为空值时拆箱报异常
        return BooleanUtil.isTrue(isLock);
    }
*/
   /* private void unLock(String key){
         stringRedisTemplate.delete(key);
    }*/

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null){
            return Result.fail("商户id不能为空");
        }
        this.updateById(shop);
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);
        return Result.ok(shop);
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        // 1.判断是否需要根据坐标查询
        if (x == null || y == null){
            // 根据类型分页查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            // 返回数据
            return Result.ok(page.getRecords());
        }
        // 2.计算分页参数
        int form = (current-1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;
        // 3.查询redis、按照距离排序、分页。结果：shopId、distance
        String key = RedisConstants.SHOP_GEO_KEY + typeId;
        // GEOSEARCH key bylonlat x y byradius 10 withdistancd
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo()
                .search(
                        key,
                        GeoReference.fromCoordinate(x, y),
                        new Distance(5000),
                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end));
        // 4.解析出id
        if (results == null){
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        // 没有下一页了，结束
        if (list.size() <= form){
            return Result.ok(Collections.emptyList());
        }
        // 4.1.截取 from ~ end的部分
        List<Long> ids = new ArrayList<>(list.size());
        Map<String,Distance> distanceMap = new HashMap<>(list.size());
        list.stream().skip(form).forEach(shop -> {
//            获取店铺id
            String shopIdStr = shop.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
//            获取店铺距离
            Distance distance = shop.getDistance();
            distanceMap.put(shopIdStr,distance);
        });
        // 5.根据id查询Shop
        String idStr = StrUtil.join(",",ids);
        List<Shop> shops = query().in("id", ids).last("order by field(id," + idStr + ")").list();
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        // 6.返回
        return Result.ok(shops);
    }
}
