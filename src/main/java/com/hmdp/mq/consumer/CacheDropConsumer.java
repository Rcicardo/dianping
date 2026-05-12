package com.hmdp.mq.consumer;// 文件位置：com.hmdp.mq.consumer.CacheDropConsumer

import com.hmdp.utils.CacheClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;
import org.apache.rocketmq.spring.annotation.MessageModel;

import javax.annotation.Resource;

@Slf4j
@Component
@RocketMQMessageListener(
        topic = "SHOP_CACHE_DROP_TOPIC",          // 必须与发送端一致
        consumerGroup = "hmdp_cache_drop_group",   // 每个节点启动后都会加入这个组
        messageModel = MessageModel.BROADCASTING   // 【核心】广播模式，确保每个 JVM 都能收到
)
public class CacheDropConsumer implements RocketMQListener<String> {

    @Resource
    private CacheClient cacheClient;

    @Override
    public void onMessage(String cacheKey) {
        log.info("监听到缓存失效通知，正在清理本地缓存: {}", cacheKey);
        // 调用我们之前在 CacheClient 中定义的清理方法
        cacheClient.invalidateLocal(cacheKey);
    }
}