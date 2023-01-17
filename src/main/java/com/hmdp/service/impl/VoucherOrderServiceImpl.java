package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWork;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Autowired
    private ISeckillVoucherService iSeckillVoucherService;
    @Resource
    private RedisIdWork redisIdWork;
    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT ;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }
    IVoucherOrderService proxy =null;
//    异步处理线程池
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    @PostConstruct
    private void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }
    private class VoucherOrderHandler implements Runnable {
        @Override
        public void run() {
            String queueName = "stream.orders";
            while (true){
                try {
//                1.获取消息队列中的订单信息 XREADGROUP group  g1 c1 count 1 blocking 2000 streams stream.orders
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed()));
//                2.验证是否还有消息
                    if (list == null || list.isEmpty()){
//                        如果获取失败，说明没有消息，继续下一次循环
                        continue;
                    }
//                3.解析订单消息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(),true);
//                4.消息应答 XACK
                    stringRedisTemplate.opsForStream().acknowledge("s1","g1",record.getId());
//                    2.创建订单
                    handleVoucherOrder(voucherOrder);

                } catch (Exception e) {
                    log.error("处理订单异常",e);
//                    处理pending-list消息
                    handlePendingList();
                }
            }
    }

        private void handlePendingList() {
            while (true) {
                try {
//                1.获取消息队列中的订单信息 XREADGROUP group  g1 c1 count 1 blocking 2000 streams stream.orders
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create("stream.orders", ReadOffset.from("0")));
//                2.验证是否还有消息
                    if (list == null || list.isEmpty()) {
//                        如果获取失败，说明没有消息，结束循环
                        break;
                    }
//                3.解析订单消息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
//                4.消息应答 XACK
                    stringRedisTemplate.opsForStream().acknowledge("s1", "g1", record.getId());
//                    2.创建订单
                    handleVoucherOrder(voucherOrder);

                } catch (Exception e) {
                    log.error("处理pending-list异常", e);
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }

                }
            }
        }

/*    //    阻塞队列
    private static final BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024*1024);
    // 用于线程池处理的任务
// 当初始化完毕后，就会去从对列中去拿信息
    private class VoucherOrderHandler implements Runnable {

        @Override
        public void run() {
            while (true){
                try {
//                获取队列中的订单信息
                    VoucherOrder voucherOrder = orderTasks.take();
//                    2.创建订单
                    handleVoucherOrder(voucherOrder);

                } catch (InterruptedException e) {
                    log.error("处理订单异常",e);
                }
            }
        }*/


        private void handleVoucherOrder(VoucherOrder voucherOrder) {
//           1. 获取用户
            Long userId = voucherOrder.getUserId();
            if (userId == null){
                return;
            }
//            2.创建锁对象
            RLock lock = redissonClient.getLock("local:order:" + userId);
//            3.尝试获取锁
            boolean isLock = lock.tryLock();
            if (!isLock){
                log.error("不允许重复下单");
                return;
            }
            try {
                //注意：由于是spring的事务是放在threadLocal中，此时的是多线程，事务会失效
                proxy.createVoucherOrder(voucherOrder);
            } finally {
                // 释放锁
                lock.unlock();
            }
        }
    }

  @Override
    public Result seckillVoucher(Long voucherId) {
      //        获取用户
      Long userId = UserHolder.getUser().getId();
      long orderId = redisIdWork.nextId("order");
//        执行lua脚本
      Long result = stringRedisTemplate.execute(
              SECKILL_SCRIPT,
              Collections.emptyList(),
              voucherId.toString(),
              userId.toString(),String.valueOf(orderId)
      );
      if (result == null){
          return Result.fail("下单失败");
      }
      int r = result.intValue();
      if (r != 0) {
          return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
      }
      // 3.返回订单id
      return Result.ok(orderId);
  }

   /* @Override
    public Result seckillVoucher(Long voucherId) {
//        获取用户
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWork.nextId("order");
//        执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString(),String.valueOf(orderId)
        );
        int r = result.intValue();
        if (r != 0){
            return Result.fail(r == 1 ? "库存不足":"不能重复下单");
        }
        //TODO 保存阻塞队列
        VoucherOrder voucherOrder = new VoucherOrder();

//        订单id
        voucherOrder.setId(orderId);
//        用户id
        voucherOrder.setUserId(userId);
//        代金券id
        voucherOrder.setVoucherId(voucherId);
//        放入阻塞队列
        orderTasks.add(voucherOrder);
        //3.获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();

        // 3.返回订单id
        return Result.ok(orderId);
    }*/
//    @Override
//    public Result seckillVoucher(Long voucherId) {
////        1.查询优惠券信息
//        SeckillVoucher seckillVoucher = iSeckillVoucherService.getById(voucherId);
////        2.判断秒杀是否开始
//        LocalDateTime now = LocalDateTime.now();
//        if (seckillVoucher.getBeginTime().isAfter(now)) {
//            return Result.fail("秒杀未开始");
//        }
//        if (seckillVoucher.getEndTime().isBefore(now)){
//            return Result.fail("秒杀已结束");
//        }
////        3.是否还有库存
//        if (seckillVoucher.getStock()<1) {
//            return Result.fail("库存不足");
//        }
//
//
////      一人一单
//        Long userId = UserHolder.getUser().getId();
////        intern在底层寻找值相等的对  象，不回去创建对象
////        synchronized (userId.toString().intern()) {
//////            获得代理对象
////            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
////            return proxy.createVoucherOrder(voucherId);
////        }
//
////        使用redis实现分布式锁，解决集群的锁问题
//        //创建锁对象 这个代码不用了，因为我们现在要使用分布式锁
////        SimpleRedisLock lock = new SimpleRedisLock("order:"+userId, stringRedisTemplate);
//
//        RLock lock = redissonClient.getLock("lock:order:" + userId);
//
//        boolean isLock = lock.tryLock();
//        if (!isLock){
//            return Result.fail("不允许重复下单");
//        }
//        try {
//            //         获得代理对象
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        }finally {
//            //释放锁
//            lock.unlock();
//        }
//    }

    @Override
    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder){
//        4.一人一单
        Long userId = voucherOrder.getUserId();
//          查询订单
        int count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
        if (count > 0){
            log.error("用户已购买");
            return;
        }
//        5.扣减库存，创建订单
        boolean success = iSeckillVoucherService.update()
                .setSql("stock = stock -1")
                .eq("voucher_id", voucherOrder.getVoucherId()).gt("stock",0 )
                .update();
        if (!success){
            log.error("库存不足");
            return;
        }
        save(voucherOrder);
//        VoucherOrder voucherOrder = new VoucherOrder();
////        生成订单id
//        long orderId = redisIdWork.nextId("order");
//        voucherOrder.setId(orderId);
////        用户id
//        voucherOrder.setUserId(userId);
////        代金券id
//        voucherOrder.setVoucherId(voucherOrder);
//        save(voucherOrder);
//        return Result.ok(orderId);
    }


}
