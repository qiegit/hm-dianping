package com.hmdp.config;


import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @Author 郄
 * @Date 2023/1/4 21:02
 * @Description:
 */
@Configuration
public class RedissonConfig {

    @Value("redis://${spring.redis.host}:${spring.redis.port}")
    private String host;
    @Bean
    public RedissonClient redissonClient(){
        // 配置
        Config config = new Config();
        config.useSingleServer().setAddress(host)
                .setPassword("123456");
        // 创建RedissonClient对象
        return Redisson.create(config);
    }


}
