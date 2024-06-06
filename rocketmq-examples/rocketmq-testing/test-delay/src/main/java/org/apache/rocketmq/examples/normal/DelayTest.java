/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.examples.normal;

import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.examples.normal.consumer.broadcast.BroadcastingChecker;
import org.apache.rocketmq.examples.normal.consumer.clustering.ClusteringChecker;
import org.apache.rocketmq.proxy.examples.common.entity.Msg;
import org.apache.rocketmq.proxy.examples.common.result.Checker;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.messaging.support.MessageBuilder;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@SpringBootApplication
public class DelayTest implements CommandLineRunner, SendCallback {
    private final Logger log = LoggerFactory.getLogger(DelayTest.class);

    @Resource
    private RocketMQTemplate rocketMQTemplate;

    @Value("${global.topic}")
    private String topic;

    public static void main(String[] args)  {
        SpringApplication.run(DelayTest.class, args);
    }

    @Override
    public void run(String... args) throws InterruptedException, ExecutionException {
        Thread.sleep(100);
        // async send
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
            for (int i = 0; i < COUNT; i++) {
                rocketMQTemplate.syncSend(topic + ":" + TAG,
                        MessageBuilder.withPayload(new Msg(ID, i)).build(), 3000000000L, 3);
            }
        });

        while (!CHECKERS.stream().allMatch(Checker::isFinished)) { }

        future.get();
        System.exit(0);
    }

    @Override
    public void onSuccess(SendResult sendResult) {
        log.debug("send message success: {}", sendResult);
    }

    @Override
    public void onException(Throwable e) {
        log.error("send message error", e);
    }

    public static final String TAG = "proxy-rocketmq-delay-test";
    public static final int COUNT = 5000000;
    public static final String ID = UUID.randomUUID().toString();

    private static final List<Checker> CHECKERS = new ArrayList<>();

    static {
        CHECKERS.add(ClusteringChecker.getInstance());
        CHECKERS.add(BroadcastingChecker.getInstance());
    }
}