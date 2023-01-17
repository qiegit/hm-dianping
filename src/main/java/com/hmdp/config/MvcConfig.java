package com.hmdp.config;

import com.hmdp.filter.LoginUserFilter;
import com.hmdp.filter.RefrechTokenInterceptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * @Author 郄
 * @Date 2022/12/27 19:24
 * @Description:
 */
@Configuration
public class MvcConfig implements WebMvcConfigurer {
    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 登录状态刷新问题：
     * 只进入特定的登录页面才刷新用户登录状态，像首页，如果一直在首页访问则到时间依旧需要登录
     * 解决：
     *     设置页面刷新拦截，刷新token有效期
     *     设置二级登录拦截，无用户时拦截
     * @param registry
     */
    @Override
    public void addInterceptors(InterceptorRegistry registry) {
//        登录拦截器
        registry.addInterceptor(new LoginUserFilter())
                .excludePathPatterns(
                        "/shop/**",
                        "/voucher/**",
                        "/shop-type/**",
                        "/upload/**",
                        "/blog/hot",
                        "/user/code",
                        "/user/login",
                        "/user/sign",
                        "/user/**"
                ).order(1);
//        token刷新拦截器
        registry.addInterceptor(new RefrechTokenInterceptor(stringRedisTemplate)).addPathPatterns("/**").order(0);
    }
}
