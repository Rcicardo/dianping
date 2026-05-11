package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.Collections;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

/*    @Resource
    private RabbitTemplate rabbitTemplate;*/

    @Autowired
    private RocketMQTemplate rocketMQTemplate;
    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

/*    @Resource
    private RedissonClient redissonClient;*/

    /**
     * 脚本初始化
     */
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT=new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

//    //阻塞队列，线程从中获取时，如果为空，则线程阻塞
//    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();
//
//    @PostConstruct
//    private void init(){
//        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
//    }
//    private class VoucherOrderHandler implements Runnable {
//        String queueName="stream.orders";
//        @Override
//        public void run() {
//            while (true) {
//                try {
//                    //1.获取消息队列中的队列信息
//                    //XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS streams.orders >
//                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
//                            Consumer.from("g1", "c1"),
//                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
//                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
//                    );
//                    //2.判断消息获取是否成功
//                    if (list == null || list.isEmpty()) {
//                        //2.1.如果获取失败，说明没有消息，继续下一次循环
//                        continue;
//                    }
//                    //3.解析消息中的订单信息
//                    MapRecord<String, Object, Object> record = list.get(0);
//                    Map<Object, Object> values = record.getValue();
//                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
//
//                    //4.如果获取成功，可以下单
//                    handleVoucherOrder(voucherOrder);
//
//                    //5.ACK确认 SACK stream.orders g1 id
//                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
//
//                }catch (Exception e){
//                    log.error("处理订单异常",e);
//                    try {
//                        handPendingList();
//                    } catch (InterruptedException ex) {
//                        throw new RuntimeException(ex);
//                    }
//                }
//            }
//
//        }
//
//        private void handPendingList() throws InterruptedException {
//            while (true) {
//                try {
//                    //1.获取pending-list中的队列信息
//                    //XREADGROUP GROUP g1 c1 COUNT 1 STREAMS streams.orders 0
//                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
//                            Consumer.from("g1", "c1"),
//                            StreamReadOptions.empty().count(1),
//                            StreamOffset.create(queueName, ReadOffset.from("0"))
//                    );
//                    //2.判断消息获取是否成功
//                    if (list == null || list.isEmpty()) {
//                        //2.1.如果获取失败，说明pending-list没有消息，结束循环
//                        break;
//                    }
//                    //3.解析消息中的订单信息
//                    MapRecord<String, Object, Object> record = list.get(0);
//                    Map<Object, Object> values = record.getValue();
//                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
//
//                    //4.如果获取成功，可以下单
//                    handleVoucherOrder(voucherOrder);
//
//                    //5.ACK确认 SACK stream.orders g1 id
//                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
//                }catch (Exception e){
//                    log.error("处理pending-list订单异常",e);
//                    Thread.sleep(20);
//                }
//            }
//        }
//    }
   /* private BlockingQueue<VoucherOrder> orderTasks=new ArrayBlockingQueue<>(1024*1024);
    private class VoucherOrderHandler implements Runnable {

        @Override
        public void run() {
            while (true) {
                try {
                    //1.获取队列中的队列信息
                    VoucherOrder order = orderTasks.take();
                    //2.创建订单
                    handleVoucherOrder(order);

                } catch (InterruptedException e) {
                    log.error("处理订单异常", e);
                }

            }

        }
    }*/

        /*public void handleVoucherOrder(VoucherOrder voucherOrder) {
            //1.获取用户
            Long userId = voucherOrder.getUserId();
            //2.创建锁对象
            RLock lock = redissonClient.getLock("lock:order:" + userId);
            //3.获取锁
            boolean isLock = lock.tryLock();
            //4.判断是否获取锁成功
            if(!isLock) {
                //失败，返回错误或重试
               log.error("不允许重复下单");
               return;
            }
            try {
                //直接调用，不会触发spring aop的事务管理
                //要通过代理调用，获取代理对象，才会被spring aop拦截
                proxy.createVoucherOrder(voucherOrder);
            } catch (IllegalStateException e) {
                throw new RuntimeException(e);
            }finally {
                //释放锁
                lock.unlock();
            }
        }*/
    private IVoucherOrderService proxy;
    @Override
    public Result seckillVoucher(Long voucherId) {
        //获取用户id
        Long userId = UserHolder.getUser().getId();
        //获取订单id
        long orderId = redisIdWorker.nextId("order");
        //1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(),String.valueOf(orderId)
        );
        //2.判断结果是否为0
        int r = 0;
        if (result != null) {
            r = result.intValue();
        }
        if(r!=0){
            //2.1.不为0，代表没有购买资格
            return Result.fail(r==1?"库存不足":"不能重复下单");
        }
//        //3.获取代理对象
//        proxy = (IVoucherOrderService) AopContext.currentProxy();
//        //4.返回订单id
//        return Result.ok(orderId);
        // 6. 走到这里说明 r == 0，即：Redis 预扣库存成功 + 资格校验通过
        // 封装订单对象，准备发往 RocketMQ
        VoucherOrder voucherOrder = new VoucherOrder();
        voucherOrder.setId(orderId);
        voucherOrder.setUserId(userId);
        voucherOrder.setVoucherId(voucherId);
// 直接发送对象，RocketMQ Starter 会自动帮你转 JSON
        try {
            // 7. 发送异步消息
            // 参数一：Topic 名字，建议在常量类里定义
            // 参数二：订单对象，RocketMQTemplate 会自动将其转为 JSON 字符串发送
            rocketMQTemplate.syncSendOrderly(
                    "seckill_order_topic",
                    voucherOrder,
                    String.valueOf(voucherOrder.getVoucherId()) // 这是一个“路标”
            );
        } catch (Exception e) {
            log.error("发送 RocketMQ 消息失败，订单ID：{}", orderId, e);
            // 这里其实可以不抛异常，通过后续的补偿机制处理，但为了严谨先保留
            throw new RuntimeException("发送订单消息失败");
        }
        // 3. 返回订单号给前端（实际下单异步处理）
        return Result.ok(orderId);
    }


//    public Result seckillVoucher(Long voucherId) {
//        //查询用户券信息
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//        //判断秒杀时间
//        //是否开始
//        LocalDateTime beginTime = voucher.getBeginTime();
//        if(beginTime.isAfter(LocalDateTime.now())){
//            return Result.fail("秒杀尚未开始！");
//        }
//        //是否结束
//        LocalDateTime endTime = voucher.getEndTime();
//        if(endTime.isBefore(LocalDateTime.now())){
//            return Result.fail("秒杀已经结束");
//        }
//        //判断库存呢是否充足
//        if(voucher.getStock()<=0){
//            return Result.fail("库存不足！");
//        }
//        Long userId = UserHolder.getUser().getId();
//       //创建锁对象
//        //SimpleRedisLock  lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
//        RLock lock = redissonClient.getLock("lock:order:" + userId);
//        //获取锁
//        boolean isLock = lock.tryLock();
//        //判断是否获取锁成功
//        if(!isLock) {
//            //失败，返回错误或重试
//            return Result.fail("不允许重复下单");
//
//        }
//        try {
//            //直接调用，不会触发spring aop的事务管理
//            //要通过代理调用，获取代理对象，才会被spring aop拦截
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        } catch (IllegalStateException e) {
//            throw new RuntimeException(e);
//        }finally {
//            //释放锁
//            lock.unlock();
//        }
//
//
//    }

    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        // 1. 幂等检查 (针对订单 ID，防止 MQ 重复消费)
        VoucherOrder oldOrder = getById(voucherOrder.getId());
        if (oldOrder != null) {
            return;
        }

        // 2. 一人一单校验 (虽然有唯一索引，保留查询可以打印更友好的日志)
        Long userId = voucherOrder.getUserId();
        int count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
        if (count > 0) {
            log.error("用户：{} 已经购买过优惠券：{}，重复下单请求已拦截", userId, voucherOrder.getVoucherId());
            return;
        }

        // 3. 扣减库存 (乐观锁)
        boolean success = seckillVoucherService
                .update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId())
                .gt("stock", 0) // 防止超卖的核心
                .update();

        if (!success) {
            log.error("库存扣减失败，券ID：{}", voucherOrder.getVoucherId());
            return;
        }

        // 4. 保存订单
        try {
            save(voucherOrder);
        } catch (Exception e) {
            // 万一前面的 count 没拦住（并发极高时），数据库唯一索引会在这里抛异常
            log.error("数据库唯一索引拦截：用户 {} 重复下单", userId);
        }


        /*// 1.幂等性检查:利用主键查询
        VoucherOrder oldOrder = getById(voucherOrder.getId());
        if (oldOrder != null){
            //如果订单存在,说明是重复消息,直接返回,不再落库
            log.warn("订单已存在,忽略重复消息:{}", voucherOrder.getId());
            return;
        }
        //一人一单
        //查询订单
        Long userId =voucherOrder.getUserId();
            int count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
            //判断是否存在
            if (count > 0) {
                //用户已经购买过了
                log.error("用户已经购买过一次了");
                return;
            }
            //扣减库存
            boolean success = seckillVoucherService
                    .update()
                    .setSql("stock=stock-1")
                    .eq("voucher_id", voucherOrder.getVoucherId())
                    .gt("stock", 0)
                    .update();
            if (!success) {
                log.error("库存不足");
                return ;
            }

            save(voucherOrder);
*/
    }
}
