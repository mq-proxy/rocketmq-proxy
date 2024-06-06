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
package org.apache.rocketmq.examples.producer;

import org.apache.rocketmq.proxy.examples.common.client.ClientConstants;
import org.apache.rocketmq.proxy.examples.common.client.ProducerConstants;
import org.apache.rocketmq.proxy.examples.common.tools.EnvTool;
import org.apache.rocketmq.proxy.examples.common.tools.Statistic;
import org.apache.rocketmq.proxy.examples.common.tools.Unirate;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.BufferedReader;
import java.io.File;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

@SpringBootApplication
public class TestProducer implements CommandLineRunner, Runnable, TransactionListener {
    private static final Logger log = Logger.getLogger(TestProducer.class.getName());
    // Constants
    private static final int THREADS =
            EnvTool.envWithDefault("rocketmq.threads","ROCKETMQ_THREADS", 1);
    private static final int PERIOD =
            EnvTool.envWithDefault("rocketmq.period","ROCKETMQ_PERIOD", 1);
    private static final String RATE_FILE =
            EnvTool.envWithDefault("rocketmq.rate.file","ROCKETMQ_RATE_FILE",
                    System.getProperty("user.home") + File.separator + ".rocketmq-rate.txt");

    private final List<TestProducer> testProducerList = new ArrayList<>();
    private volatile Statistic timeStatistic = new Statistic();
    private volatile Statistic sizeStatistic = new Statistic();
    private static final ScheduledExecutorService EXECUTOR_SERVICE = new ScheduledThreadPoolExecutor(1);
    private static final AtomicInteger I = new AtomicInteger(0);

    public static void main(String[] args)  {
        SpringApplication.run(TestProducer.class, args);
    }

    @Override
    public void run(String[] args) {
        EXECUTOR_SERVICE.scheduleAtFixedRate(() -> {
            Statistic timeStatistic = Statistic.statistics(testProducerList.stream().map(p -> {
                Statistic statistic = p.timeStatistic;
                p.timeStatistic = new Statistic();
                return statistic;
            }).toArray(Statistic[]::new));

            Statistic sizeStatistic = Statistic.statistics(testProducerList.stream().map(p -> {
                Statistic statistic = p.sizeStatistic;
                p.sizeStatistic = new Statistic();
                return statistic;
            }).toArray(Statistic[]::new));
            log.info(String.format("size: %.2f, count: %d, time: %dms, avg: %.2fms, min: %dms, max: %dms",
                    sizeStatistic.getAvg(),
                    timeStatistic.getCount(),
                    timeStatistic.getTotal(),
                    timeStatistic.getAvg() / PERIOD,
                    timeStatistic.getMin(),
                    timeStatistic.getMax()));
        }, PERIOD, PERIOD, TimeUnit.SECONDS);

        EXECUTOR_SERVICE.scheduleAtFixedRate(() -> {
            try {
                try (BufferedReader reader = Files.newBufferedReader(Paths.get(RATE_FILE))) {
                    long qps = Long.parseLong(reader.readLine());
                    UNIRATE.setQps(qps);
                    log.info("set qps to " + qps);
                }
            } catch (Throwable e) {
                log.info("error occur: " + e.getMessage());
            }
        }, 1, 1, TimeUnit.SECONDS);

        for (int i = 0; i < THREADS; i++) {
            TestProducer testProducer = new TestProducer();
            testProducerList.add(testProducer);
            new Thread(testProducer).start();
        }
    }

    @Override
    public void run() {
        DefaultMQProducer producer;
        if (ProducerConstants.TRANSACTION) {
            TransactionMQProducer transactionMQProducer = new TransactionMQProducer(
                    ClientConstants.NAMESPACE,
                    ProducerConstants.PRODUCER_GROUP,
                    null,
                    ClientConstants.ENABLE_TRACE,
                    ClientConstants.TRACE_TOPIC);
            transactionMQProducer.setTransactionListener(this);
            producer = transactionMQProducer;
        } else {
            producer = new DefaultMQProducer(
                    ClientConstants.NAMESPACE,
                    ProducerConstants.PRODUCER_GROUP,
                    null,
                    ClientConstants.ENABLE_TRACE,
                    ClientConstants.TRACE_TOPIC);
        }
        producer.setInstanceName("inst_" + I.incrementAndGet());
        producer.setNamesrvAddr(ClientConstants.NAMESERVER);

        try {
            producer.start();
        } catch (MQClientException e) {
            throw new RuntimeException(e);
        }

        for (long i = 0; i < Long.MAX_VALUE; i++) {
            try {
                Message msg = new Message(ClientConstants.TOPIC /* Topic */,
                        ProducerConstants.MSG_TAG /* Tag */,
                        (ProducerConstants.MSG_PREFIX + i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
                );

                if (!UtilAll.isBlank(ProducerConstants.MSG_KEY)) {
                    msg.setKeys(ProducerConstants.MSG_KEY);
                }

                long start = System.currentTimeMillis();
                SendResult sendResult = producer.send(msg);
                long end = System.currentTimeMillis();
                long timeDiff = end - start;
                timeStatistic.record(timeDiff);
                sizeStatistic.record(msg.getBody().length);

                if (sendResult.getSendStatus() != SendStatus.SEND_OK) {
                    log.warning(sendResult.toString());
                }

                UNIRATE.run(next -> { });
            } catch (Throwable e) {
                log.log(Level.WARNING, e.getMessage(), e);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
        producer.shutdown();
    }

    @Override
    public LocalTransactionState executeLocalTransaction(Message message, Object o) {
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            return LocalTransactionState.ROLLBACK_MESSAGE;
        }
        return LocalTransactionState.COMMIT_MESSAGE;
    }

    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
        return LocalTransactionState.COMMIT_MESSAGE;
    }

    private static final Unirate UNIRATE = new Unirate(10000);

    public static void setQps(int qps) {
        try (PrintWriter bw = new PrintWriter(Files.newBufferedWriter(Paths.get(RATE_FILE)))) {
            bw.write(String.valueOf(qps));
        } catch (Exception e) {
            log.info("set qps error occur: " + e.getMessage());
        }
        UNIRATE.setQps(qps);
    }
}
