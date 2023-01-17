package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.toolkit.StringUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * @Author 郄
 * @Date 2023/1/2 21:36
 * @Description:
 */
@Slf4j
@Component
public class CacheClient {

    private final StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

//    redis 输入，完成自动转换
    public void set(String key, Object value, Long time, TimeUnit timeUnit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value),time,timeUnit);
    }

    /**
     * 设置逻辑过期时间的redis
     * @param key
     * @param value
     * @param time
     * @param timeUnit
     */
    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit timeUnit){
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(timeUnit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    /**
     *  防止缓存穿透的通用工具类
     * @param keyPrefix 前缀
     * @param id  商户id
     * @param type  根据用户输入的类型返回
     * @param dbFunction  数据库执行的查询，查询数据由调用者执行
     * @param time 超时时间
     * @param timeUnit
     * @param <R>
     * @param <ID>
     * @return
     */
    public <R,ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type, Function<ID,R> dbFunction,Long time, TimeUnit timeUnit){
        String key = keyPrefix + id;
//        判断redis是否有数据，有则直接返回
        String json = stringRedisTemplate.opsForValue().get(key);
        if (StringUtils.isNotBlank(json)) {
            return JSONUtil.toBean(json, type);
        }
//        判断查到的数据是否为空值,"" 则直接返回null，防止缓存穿透
        if (json != null){
            return null;
        }
        R r = dbFunction.apply(id);
//        没有查到用户，缓存填入空值，防止缓存穿透
        if (r == null){
            stringRedisTemplate.opsForValue().set(key, "",time,timeUnit);
            return null;
        }
//        写入redis
        this.set(key, r, time, timeUnit);

        return r;
    }

    /**
     * 防止缓存击穿的通用工具类
     */
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
    public <R,ID> void queryWithLogicalExpire(String keyPrefix, ID id, Class<R> type, Function<ID,R> dbFunction, Long time, TimeUnit timeUnit) {
        String key = keyPrefix + id;
//        1.从redis查询缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        if (StringUtils.isBlank(json)) {
            return;
        }
//        2.命中
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        JSONObject jsonObject = (JSONObject) redisData.getData();
        R r = JSONUtil.toBean(jsonObject, type);
//        3.判断是否过期
        if (redisData.getExpireTime().isAfter(LocalDateTime.now())){
            // 5.1.未过期，直接返回店铺信息
            return;
        }
//        过期尝试获取锁，开启新线程完成缓存重建
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        boolean tryLock = tryLock(lockKey);
        if (tryLock){

            CACHE_REBUILD_EXECUTOR.submit(() ->{
                try {
//                   重建缓存
                    R newR = dbFunction.apply(id);
                    this.setWithLogicalExpire(key, newR ,time ,timeUnit );
                }catch (Exception e){
                    throw new RuntimeException(e);
                }finally {
                    unLock(lockKey);
                }
            });
        }
//        返回过期的商品数据
    }
    private boolean tryLock(String key){
        Boolean isLock = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", RedisConstants.LOCK_SHOP_TTL, TimeUnit.SECONDS);
//        防止Boolean为空值时拆箱报异常
        return BooleanUtil.isTrue(isLock);
    }

    private void unLock(String key){
        stringRedisTemplate.delete(key);
    }
}
