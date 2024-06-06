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
package org.apache.rocketmq.examples.consumer;

import org.apache.rocketmq.proxy.examples.common.client.ClientConstants;
import org.apache.rocketmq.proxy.examples.common.client.ConsumerConstants;
import org.apache.rocketmq.proxy.examples.common.tools.EnvTool;
import org.apache.rocketmq.proxy.examples.common.tools.Statistic;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

@SpringBootApplication
public class TestConsumer implements CommandLineRunner {
    private final Logger log = Logger.getLogger(TestConsumer.class.getName());
    private final AtomicInteger instanceId = new AtomicInteger(0);
    // timer thread pool
    private final ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1);
    private volatile Statistic statistic = new Statistic();
    private static final int THREADS =
            EnvTool.envWithDefault("rocketmq.threads","ROCKETMQ_THREADS", 2);

    private static final String CONSUME_MODE =
            EnvTool.envWithDefault("rocketmq.consume-mode","ROCKETMQ_CONSUME_MODE", "CONCURRENCY");

    private static final String MESSAGE_MODEL =
            EnvTool.envWithDefault("rocketmq.message-model","ROCKETMQ_MESSAGE_MODEL", "CLUSTERING");

    public static void main(String[] args)   {
        SpringApplication.run(TestConsumer.class, args);
    }

    @Override
    public void run(String[] args) {
        for (int i = 0; i < THREADS; i++) {
            run0();
        }
        executorService.scheduleAtFixedRate(() -> {
            Statistic oldStatistic = this.statistic;
            this.statistic = new Statistic();
            log.info(String.format("count: %d, time: %dms, avg: %fms, min: %dms, max: %dms",
                    oldStatistic.getCount(),
                    oldStatistic.getTotal(),
                    oldStatistic.getAvg(),
                    oldStatistic.getMin(),
                    oldStatistic.getMax()));
        }, 1, 1, TimeUnit.SECONDS);
    }

    public void run0() {
        boolean orderly = "ORDERLY".equals(CONSUME_MODE);
        boolean broadcast = "BROADCAST".equals(MESSAGE_MODEL);
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(
                ClientConstants.NAMESPACE,
                ConsumerConstants.CONSUMER_GROUP,
                null,
                new AllocateMessageQueueAveragely(),
                ClientConstants.ENABLE_TRACE,
                ClientConstants.TRACE_TOPIC);
        consumer.setNamesrvAddr(ClientConstants.NAMESERVER);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);

        if (broadcast) {
            consumer.setMessageModel(MessageModel.BROADCASTING);
            consumer.setInstanceName("" + instanceId.incrementAndGet());
        } else {
            consumer.setMessageModel(MessageModel.CLUSTERING);
        }

        try {
            consumer.subscribe(ClientConstants.TOPIC, ConsumerConstants.SUBSCRIPTION);
        } catch (MQClientException e) {
            throw new RuntimeException(e);
        }

        if (orderly) {
            consumer.registerMessageListener(new OrderlyMessageListener());
        } else {
            consumer.registerMessageListener(new ConcurrentMessageListener());
        }

        try {
            consumer.start();
            Runtime.getRuntime().addShutdownHook(new Thread(consumer::shutdown));
        } catch (MQClientException e) {
            throw new RuntimeException(e);
        }
        System.out.printf("Consumer Started.%n");
    }

    private boolean consumeMessage(List<MessageExt> msgs) {
        for (MessageExt msg : msgs) {
            long timeDiff = System.currentTimeMillis() - msg.getBornTimestamp();
            statistic.record(timeDiff);
        }
        return true;
    }

    private class ConcurrentMessageListener implements MessageListenerConcurrently {
        @Override
        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
            return TestConsumer.this.consumeMessage(msgs) ?
                    ConsumeConcurrentlyStatus.CONSUME_SUCCESS : ConsumeConcurrentlyStatus.RECONSUME_LATER;
        }
    }

    private class OrderlyMessageListener implements MessageListenerOrderly {
        @Override
        public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
            return TestConsumer.this.consumeMessage(msgs) ?
                    ConsumeOrderlyStatus.SUCCESS : ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
        }
    }
}
