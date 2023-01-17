package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import javax.servlet.http.HttpSession;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.USER_SIGN_KEY;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {
    @Autowired
    private StringRedisTemplate stringRedisTemplate;


    @Override
    public Result sendCode(String phone, HttpSession session) {
        //        验证手机号
        if (RegexUtils.isPhoneInvalid(phone)) {
            return Result.fail("手机号格式不正确");
        }
//        生成验证码
        String code = RandomUtil.randomNumbers(6);
        stringRedisTemplate.opsForValue().set(RedisConstants.LOGIN_CODE_KEY + phone, code,RedisConstants.LOGIN_CODE_TTL, TimeUnit.MINUTES);
        return Result.ok();
    }

    @Override
    public Result sign() {
//        1.获取用户id
        Long userId = UserHolder.getUser().getId();
//        2.获取签到日期
        LocalDateTime now = LocalDateTime.now();
//        3.拼接key
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key = USER_SIGN_KEY + userId +keySuffix;
//        4.获取今天是本月的第几天
        int dayOfMonth = now.getDayOfMonth();
//        5.写入redis  setBIT key offset 1
        stringRedisTemplate.opsForValue().setBit(key,dayOfMonth-1,true);

        return Result.ok();
    }

    @Override
    public Result signCount() {
        //        1.获取用户id
        Long userId = UserHolder.getUser().getId();
//        2.获取签到日期
        LocalDateTime now = LocalDateTime.now();
//        3.拼接key
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key = USER_SIGN_KEY + userId +keySuffix;
//        4.获取今天是本月的第几天
        int dayOfMonth = now.getDayOfMonth();
//        5.查询总的签到次数，返回的是一个十进制  BITFIELD sign:5:202203 GET u14 0
        List<Long> results = stringRedisTemplate.opsForValue().bitField(
                key,
                BitFieldSubCommands.create()
                        .get(BitFieldSubCommands.BitFieldType.unsigned(dayOfMonth)).valueAt(0));
        if (results == null || results.isEmpty()){
            return Result.ok(0);
        }
        Long num = results.get(0);
        if (num ==null || num == 0){
            return Result.ok(0);
        }
//        6。遍历签到次数，与1做与运算，找出最后连续签到的次数
        int count = 0;
        while (true){
            // 6.1.让这个数字与1做与运算，得到数字的最后一个bit位  // 判断这个bit位是否为0
            if ((num & 1) == 0) {
                break;
            }else {
                // 如果不为0，说明已签到，计数器+1
                count++;
            }
            // 把数字右移一位，抛弃最后一个bit位，继续下一个bit位
            num >>>=1;
        }
        return Result.ok(count);
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
//        验证手机号格式
        String phone = loginForm.getPhone();
        if (RegexUtils.isPhoneInvalid(phone)) {
            return Result.fail("手机号不正确");
        }
//        验证验证码是否正确

        String cacheCode = stringRedisTemplate.opsForValue().get(RedisConstants.LOGIN_CODE_KEY + phone);
        String code = loginForm.getCode();
        if (code==null || !code.equals(cacheCode)) {
            return Result.fail("验证码不正确");
        }
//      判断用户时候已注册
        User user = query().eq("phone", phone).one();
        if (user == null){
            user = createUserWithPhone(phone);
        }
//       生成token
        String token = UUID.randomUUID().toString();
//        保存用户到redis
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        Map<String,Object> userMap = BeanUtil.beanToMap(userDTO,new HashMap<>(),
                CopyOptions.create().
                        ignoreNullValue().
                        setFieldValueEditor((fieldName,fieldValue) -> fieldValue.toString()));
        stringRedisTemplate.opsForHash().putAll(RedisConstants.LOGIN_USER_KEY + token,userMap);
        stringRedisTemplate.expire(RedisConstants.LOGIN_USER_KEY + token,RedisConstants.LOGIN_USER_TTL,TimeUnit.MINUTES);

        // TODO 实现登录功能
        return Result.ok(token);
    }



    private User createUserWithPhone(String phone) {
        User user = new User();
        user.setPhone(phone);
        user.setNickName(SystemConstants.USER_NICK_NAME_PREFIX+ RandomUtil.randomString(4));
        this.save(user);
        return user;
    }
}
