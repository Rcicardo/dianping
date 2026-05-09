package com.hmdp;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.Assert;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.thread.ThreadUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.service.IUserService;
import lombok.Builder;
import lombok.SneakyThrows;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

import javax.annotation.Resource;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@SpringBootTest
@AutoConfigureMockMvc
class VoucherOrderControllerTest {

    @Resource
    private MockMvc mockMvc;

    @Resource
    private IUserService userService;

    @Resource
    private ObjectMapper mapper;

    @Resource
    StringRedisTemplate stringRedisTemplate;



    @Test
    @SneakyThrows
    @DisplayName("登录1000个用户，并输出到文件中")
    void login() {
        List<String> phoneList = userService.lambdaQuery()
                 .select(User::getPhone)
                .last("limit 1000")
                .list().stream().map(User::getPhone).collect(Collectors.toList());
        ExecutorService executorService = ThreadUtil.newExecutor(phoneList.size());
        List<String> tokenList = new CopyOnWriteArrayList<>();
        CountDownLatch countDownLatch = new CountDownLatch(phoneList.size());
        phoneList.forEach(phone -> {
            executorService.execute(() -> {
                try {
                    // 验证码
                    String codeJson = mockMvc.perform(MockMvcRequestBuilders
                                    .post("/user/code")
                                    .queryParam("phone", phone))
                            .andExpect(MockMvcResultMatchers.status().isOk())
                            .andReturn().getResponse().getContentAsString();
                    Result result = mapper.readerFor(Result.class).readValue(codeJson);
                    Assert.isTrue(result.getSuccess(), String.format("获取“%s”手机号的验证码失败", phone));
                    String code = result.getData().toString();
                    LoginFormDTO formDTO = LoginFormDTO.builder().code(code).phone(phone).build();
                    String json = mapper.writeValueAsString(formDTO);
                    // token
                    String tokenJson = mockMvc.perform(MockMvcRequestBuilders
                                    .post("/user/login").content(json).contentType(MediaType.APPLICATION_JSON))
                            .andExpect(MockMvcResultMatchers.status().isOk())
                            .andReturn().getResponse().getContentAsString();
                    result = mapper.readerFor(Result.class).readValue(tokenJson);
                    Assert.isTrue(result.getSuccess(), String.format("获取“%s”手机号的token失败,json为“%s”", phone, json));
                    String token = result.getData().toString();
                    tokenList.add(token);
                    countDownLatch.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        });
        countDownLatch.await();
        executorService.shutdown();
        Assert.isTrue(tokenList.size() == phoneList.size());
        writeToTxt(tokenList, "\\tokens.txt");
        System.out.println("写入完成！");
    }

    private static void writeToTxt(List<String> list, String suffixPath) throws Exception {
        // 1. 创建文件
        File file = new File(System.getProperty("user.dir") + "\\src\\main\\resources" + suffixPath);
        if (!file.exists()) {
            file.createNewFile();
        }
        // 2. 输出
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), StandardCharsets.UTF_8));
        for (String content : list) {
            bw.write(content);
            bw.newLine();
        }
        bw.close();
        System.out.println("写入完成！");
    }


/*    @Test
    void testQuickExportTokens() throws IOException {
        // 1. 获取 1000 个手机号（确保数据库里已经有这些用户）
        List<User> userList = userService.query().last("limit 1000").list();
        if (userList.isEmpty()) {
            System.out.println("数据库里没用户，请先运行你的造数据脚本！");
            return;
        }

        // 2. 准备导出文件
        // 文件会生成在项目根目录下，方便你一会儿在 JMeter 里直接引用名：tokens.txt
        FileWriter writer = new FileWriter("tokens.txt");
        BufferedWriter bufferedWriter = new BufferedWriter(writer);

        System.out.println("开始批量注入 Redis 并生成 Token...");

        for (User user : userList) {
            // 3. 核心：直接生成 Token，不走验证码流程
            String token = UUID.randomUUID().toString(true);

            // 4. 将用户信息转为 DTO 并存入 Map (逻辑必须与你的拦截器一致)
            UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
            // 4. 将用户信息转为 Map (直接使用简单的 beanToMap)
            Map<String, Object> userMap = BeanUtil.beanToMap(userDTO, new HashMap<>(),
                    CopyOptions.create()
                            .setIgnoreNullValue(true)
                            .setFieldValueEditor((name, value) -> value != null ? value.toString() : ""));

            // 5. 拼装 Redis Key (请确认你的拦截器里前缀是什么，通常是 login:token:)
            String tokenKey = "login:token:" + token;

            // 6. 直接写入 Redis
            stringRedisTemplate.opsForHash().putAll(tokenKey, userMap);
            // 设置有效期 30 分钟，足够压测使用了
            stringRedisTemplate.expire(tokenKey, 30, TimeUnit.MINUTES);

            // 7. 写入文件
            bufferedWriter.write(token);
            bufferedWriter.newLine();
        }

        bufferedWriter.close();
        System.out.println("注入成功！1000 个 Token 已保存至项目根目录下的 tokens.txt");
    }*/

    @Test
    void testQuickExportTokens() throws IOException {
        // 1. 先去数据库确认到底有多少人，我们只取实际存在的人数
        List<User> userList = userService.list();
        int actualUserCount = userList.size();
        System.out.println("数据库实际用户数: " + actualUserCount);

        // 2. 清理旧文件和旧 Redis 数据 (防止干扰)
        // 注意：如果是生产环境千万别这么干，压测环境可以执行 redis-cli flushdb

        FileWriter writer = new FileWriter("tokens.txt");
        BufferedWriter bufferedWriter = new BufferedWriter(writer);

        int count = 0;
        for (User user : userList) {
            if (count >= 1000) break; // 我们只要 1000 个

            String token = UUID.randomUUID().toString(true);
            UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
            Map<String, Object> userMap = BeanUtil.beanToMap(userDTO, new HashMap<>(),
                    CopyOptions.create()
                            .setIgnoreNullValue(true)
                            .setFieldValueEditor((name, value) -> value != null ? value.toString() : ""));

            String tokenKey = "login:token:" + token;

            // 确保写入成功
            stringRedisTemplate.opsForHash().putAll(tokenKey, userMap);
            stringRedisTemplate.expire(tokenKey, 1, TimeUnit.HOURS); // 有效期长一点

            bufferedWriter.write(token);
            bufferedWriter.newLine();
            count++;
        }
        bufferedWriter.close();
        System.out.println("最终成功注入 Redis 并写入文件的数量: " + count);
    }
}