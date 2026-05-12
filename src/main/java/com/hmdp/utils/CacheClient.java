package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static cn.hutool.json.JSONUtil.toJsonStr;

@Slf4j
@Component
public class CacheClient {
    private final StringRedisTemplate stringRedisTemplate;
    private final Cache<String, String> localCache; // 变成注入方式

    // 构造器注入
    public CacheClient(StringRedisTemplate stringRedisTemplate, Cache<String, String> localCache) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.localCache = localCache;
    }

    // --- 辅助方法：对外暴露清理接口 ---
    public void invalidateLocal(String key) {
        localCache.invalidate(key);
    }

    public void set(String key, Object value, Long time, TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, toJsonStr(value),time,unit);
    }

    public void setWithLogicalExpire(String key,Object value,Long time,TimeUnit unit){
        //设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        String jsonStr = JSONUtil.toJsonStr(redisData);
        //写入redis
        stringRedisTemplate.opsForValue().set(key,jsonStr);
        // 【建议同步写入本地】：这样不用等下次查询，本地立刻就有热数据了
        localCache.put(key, jsonStr);
    }

   /* public <R,ID> R queryWithPassThrough(
            String keyPrefix, ID id, Class<R> type, Function<ID,R> dbFallback,Long time,TimeUnit unit){
        String key=keyPrefix+id;
        //1.尝试从Redis查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //2.判断缓存是否存在
        if(StrUtil.isNotBlank(json)) { //判断字符串既不为null，也不是空字符串(""),且也不是空白字符
            //3.存在，返回商铺信息
            return JSONUtil.toBean(json, type);

        }
        //判断是否为空值
        if(json!=null){
            return null;
        }
        //4.不存在，根据id查询数据库
        R r = dbFallback.apply(id);
        //5.判断数据库中是否存在
        if(r==null){
            //6.不存在，返回错误状态码
            stringRedisTemplate.opsForValue().set(key,"",RedisConstants.CACHE_NULL_TTL,TimeUnit.MINUTES);
            return null;
        }
        //7.存在，写入redis，返回商铺信息
       this.set(key,r,time,unit);

        return r;

    }
*/
    /**
     * 缓存穿透策略：二级缓存增强版
     */
    public <R, ID> R queryWithPassThrough(
            String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        System.out.println(">>> [Debug] 业务逻辑已进入 CacheClient.queryWithPassThrough，正在查询 ID: " + id);
        String key = keyPrefix + id;

        // 1. 尝试从【一级缓存：Caffeine】获取
        String json = localCache.getIfPresent(key);
        if (StrUtil.isNotBlank(json)) {
            return JSONUtil.toBean(json, type);
        }
        // 处理穿透标记：json != null 说明命中了一级缓存的 ""
        if (json != null) {
            return null;
        }

        // 2. 尝试从【二级缓存：Redis】获取
        try {
            json = stringRedisTemplate.opsForValue().get(key);
        } catch (Exception e) {
            log.warn("Redis访问异常: {}", e.getMessage());
        }

        // 3. 判断 Redis 命中情况
        if (StrUtil.isNotBlank(json)) {
            localCache.put(key, json); // 回填一级缓存
            return JSONUtil.toBean(json, type);
        }
        // 处理 Redis 穿透标记
        if (json != null) {
            localCache.put(key, ""); // 同步至一级缓存
            return null;
        }

        // 4. 数据库回源
        R r = dbFallback.apply(id);

        // 5. 数据库不存在
        if (r == null) {
            // 缓存空对象到 Redis 和 本地
            stringRedisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
            localCache.put(key, "");
            return null;
        }

        // 6. 写入 Redis 和 本地
        String jsonStr = toJsonStr(r);
        // 注意：这里使用你类里定义的 set 方法，保持过期时间逻辑一致
        this.set(key, r, time, unit);
        localCache.put(key, jsonStr);

        return r;
    }


    private static final ExecutorService CACHE_REBUILD_EXECUTOR= Executors.newFixedThreadPool(10);
    /*public <R,ID> R queryWithLogicalExpire(
            String keyPrefix,ID id,Class<R> type,Function<ID,R> dbFallback,Long time,TimeUnit unit){
        String key=keyPrefix+id;
        //1.尝试从Redis查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //2.判断缓存是否存在
        if(StrUtil.isBlank(json)) { //判断字符串既不为null，也不是空字符串(""),且也不是空白字符
            //3.不存在，返回商铺信息
            return null;

        }

        //4.存在，将json反序列化为对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        R shop = JSONUtil.toBean((JSONObject) redisData.getData(),type);
        LocalDateTime expireTime = redisData.getExpireTime();
        //5.判断是否过期
        if(expireTime.isAfter(LocalDateTime.now())) {
            //5.1.未过期，直接返回店铺信息
            return shop;
        }
        //5.2.已过期，需要返回缓存重建
        //6.缓存重建
        //6.1.获取互斥锁
        String lockKey=RedisConstants.LOCK_SHOP_KEY+id;
        boolean isLock = tryLock(lockKey);
        //6.2.判断是否获取锁成功
        if(isLock){
            //  6.3.成功，开启独立线程实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(()->{
                try {
                   //查询数据库
                    R r1= dbFallback.apply(id);
                    //写入redis
                    this.setWithLogicalExpire(key,r1,time,unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }finally {
                    //释放锁
                    unLock(lockKey);
                }
            });

        }

        //6.4.返回过期的商铺信息
        return shop;

    }

    */

    public <R, ID> R queryWithLogicalExpire(
            String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {

        String key = keyPrefix + id;

        // 1. 尝试从【一级缓存：Caffeine】查询
        String json = localCache.getIfPresent(key);

        // 2. 如果本地没中，再查【二级缓存：Redis】
        if (StrUtil.isBlank(json)) {
            json = stringRedisTemplate.opsForValue().get(key);
        }

        // 3. 判断是否存在（逻辑过期方案要求缓存必须预热，如果彻底没有则返回空）
        if (StrUtil.isBlank(json)) {
            return null;
        }

        // 4. 反序列化为对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        R data = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();

        // 5. 判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            // 5.1 未过期，回填一级缓存（防止下次查Redis），直接返回
            localCache.put(key, json);
            return data;
        }

        // 5.2 已过期，尝试缓存重建
        // 6. 缓存重建
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);

        if (isLock) {
            // 6.1 获取锁成功，开启独立线程
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    // 查询数据库
                    R r1 = dbFallback.apply(id);
                    // 写入 Redis（setWithLogicalExpire 内部会封装 RedisData）
                    this.setWithLogicalExpire(key, r1, time, unit);

                    // 【关键新增】：重建成功后，立刻刷新一级缓存，保证其他线程后续能从内存拿到最新的逻辑过期时间
                    RedisData newData = new RedisData();
                    newData.setData(r1);
                    newData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
                    localCache.put(key, toJsonStr(newData));

                } catch (Exception e) {
                    log.error("缓存重建失败", e);
                } finally {
                    // 6.2 释放锁（注意这里是你之前修正过的 lockKey）
                    unLock(lockKey);
                }
            });
        }

        // 7. 返回过期的旧信息
        return data;
    }
    /**
     * 创建锁
     * @param key
     * @return
     */
    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    /**
     * 封闭锁
     * @param key
     */
    private void unLock(String key){
        stringRedisTemplate.delete(key);
    }
}
