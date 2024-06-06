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

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.proxy.examples.common.tools.EnvTool;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.examples.normal.consumer.broadcast.BroadcastingChecker;
import org.apache.rocketmq.examples.normal.consumer.clustering.ClusteringChecker;
import org.apache.rocketmq.proxy.examples.common.entity.Msg;
import org.apache.rocketmq.proxy.examples.common.result.Checker;
import org.apache.rocketmq.spring.annotation.RocketMQTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionState;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

@SpringBootApplication
public class TransactionTest implements CommandLineRunner {
    private final Logger log = LoggerFactory.getLogger(TransactionTest.class);

    @Resource
    private RocketMQTemplate rocketMQTemplate;

    @Value("${global.topic}")
    private String topic;

    public static void main(String[] args)  {
        SpringApplication.run(TransactionTest.class, args);
    }

    @Override
    public void run(String... args) throws InterruptedException {
        Thread.sleep(100);
        // -100 to -1  rollback (check)
        // 0, 2, 4, ...  commit
        // 1, 3, 5, ...  commit (check)
        for (int i = -100; i < COUNT; i++) {
            SendResult sendResult = rocketMQTemplate.sendMessageInTransaction(topic + ":" + TAG,
                    MessageBuilder.withPayload(new Msg(ID, i)).build(), null);
            log.debug("index: {}, {}",i, sendResult);
        }

        while (!CHECKERS.stream().allMatch(Checker::isFinished)) { }

        System.exit(0);
    }

    @RocketMQTransactionListener
    class TransactionListener implements RocketMQLocalTransactionListener {
        // local transaction, just commit even index
        @Override
        public RocketMQLocalTransactionState executeLocalTransaction(org.springframework.messaging.Message msg, Object arg) {
            return check(msg, message -> {
                if (message.getIndex() % 2 == 0 && message.getIndex() >= 0) {
                    return RocketMQLocalTransactionState.COMMIT;
                }
                return RocketMQLocalTransactionState.UNKNOWN;
            });
        }

        // check transaction, commit odd index, rollback negative index(will throw Exception when received)
        @Override
        public RocketMQLocalTransactionState checkLocalTransaction(org.springframework.messaging.Message msg) {
            log.debug("check transaction: {}", msg);
            return check(msg, message -> {
                if (message.getIndex() < 0) {
                    return RocketMQLocalTransactionState.ROLLBACK;
                } else if (message.getIndex() % 2 == 1) {
                    return RocketMQLocalTransactionState.COMMIT;
                }
                return RocketMQLocalTransactionState.UNKNOWN;
            });
        }

        private RocketMQLocalTransactionState check(Message<byte[]> message, Function<Msg, RocketMQLocalTransactionState> checker) {
            Msg msg = JSON.parseObject(message.getPayload(), Msg.class);
            log.debug("local transaction {}", msg);
            return checker.apply(msg);
        }
    }

    public static final String TAG = "proxy-rocketmq-transaction-test";
    public static final int COUNT =
            EnvTool.envWithDefault("rocketmq.test-count", "ROCKETMQ_TEST_COUNT", 5000000);
    public static final String ID = UUID.randomUUID().toString();

    private static final List<Checker> CHECKERS = new ArrayList<>();

    static {
        CHECKERS.add(ClusteringChecker.getInstance());
        CHECKERS.add(BroadcastingChecker.getInstance());
    }
}
