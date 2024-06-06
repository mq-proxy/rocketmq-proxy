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
package org.apache.rocketmq.proxy.broker;

import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.proxy.common.log.LoggerName;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

public class MockBrokerStats {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    private final long bootTimestamp;
    private volatile long recordTimestamp;
    private final AtomicLong msgPutTimeMax = new AtomicLong();
    private final AtomicLong msgGetTimeMax = new AtomicLong();
    private final AtomicLong msgPutSize = new AtomicLong();
    private final AtomicLong msgPutCount = new AtomicLong();
    private final AtomicLong msgPutFailCount = new AtomicLong();
    private final AtomicLong msgGetCount = new AtomicLong();
    private final AtomicLong msgGetFailCount = new AtomicLong();
    private volatile long msgPutTotalYesterdayMorning;
    private volatile long msgPutTotalTodayMorning;
    private volatile long msgGetTotalYesterdayMorning;
    private volatile long msgGetTotalTodayMorning;

    private volatile long sampleGet = 0;
    private volatile long samplePut = 0;
    private volatile double computedPutTPS = 0;
    private volatile double computedGetQPS = 0;

    public MockBrokerStats() {
        bootTimestamp = System.currentTimeMillis();
        recordTimestamp = System.currentTimeMillis();
    }

    public long getBootTimestamp() {
        return bootTimestamp;
    }

    public String getFormatRuntime() {
        final long millisecond = 1;
        final long second = 1000 * millisecond;
        final long minute = 60 * second;
        final long hour = 60 * minute;
        final long day = 24 * hour;
        final MessageFormat messageFormat = new MessageFormat("[ {0} days, {1} hours, {2} minutes, {3} seconds ]");

        long time = System.currentTimeMillis() - this.bootTimestamp;
        long days = time / day;
        long hours = (time % day) / hour;
        long minutes = (time % hour) / minute;
        long seconds = (time % minute) / second;
        return messageFormat.format(new Long[] {days, hours, minutes, seconds});
    }

    public void recordOneDay() {
        this.msgPutTotalYesterdayMorning = this.msgPutTotalTodayMorning;
        this.msgGetTotalYesterdayMorning = this.msgGetTotalTodayMorning;
        this.msgPutTotalTodayMorning = msgPutCount.get();
        this.msgGetTotalTodayMorning = msgGetCount.get();
        log.info("yesterday put message total: {}", this.msgPutTotalTodayMorning - this.msgPutTotalYesterdayMorning);
        log.info("yesterday get message total: {}", this.msgGetTotalTodayMorning - this.msgGetTotalYesterdayMorning);
    }

    public void recordOneSecond() {
        long before = this.recordTimestamp;
        long now = System.currentTimeMillis();
        long currentGet = msgGetCount.get();
        long currentPut = msgPutCount.get();
        long getCount = currentGet - sampleGet;
        long putCount = currentPut - samplePut;
        computedPutTPS = 1000.0 * putCount / (now - before);
        computedGetQPS = 1000.0 * getCount / (now - before);
        sampleGet = msgGetCount.get();
        samplePut = msgPutCount.get();
        recordTimestamp = now;
    }

    public long getMsgPutTotalYesterdayMorning() {
        return this.msgPutTotalYesterdayMorning;
    }

    public long getMsgPutTotalTodayMorning() {
        return this.msgPutTotalTodayMorning;
    }

    public long getMsgGetTotalYesterdayMorning() {
        return this.msgGetTotalYesterdayMorning;
    }

    public long getMsgGetTotalTodayMorning() {
        return this.msgGetTotalTodayMorning;
    }

    public long getMsgPutTotalTodayNow() {
        return this.msgPutCount.get();
    }

    public long getMsgGetTotalTodayNow() {
        return this.msgGetCount.get();
    }

    public void recordGet(long count, long time, boolean success) {
        this.msgGetCount.addAndGet(count);
        if (!success) {
            this.msgGetFailCount.addAndGet(count);
        }
        long old = this.msgGetTimeMax.get();
        while (!this.msgGetTimeMax.compareAndSet(old, Math.max(old, time))) {
            old = this.msgGetTimeMax.get();
        }
    }

    public void recordPut(long time, long size, boolean success) {
        this.msgPutCount.incrementAndGet();
        if (!success) {
            this.msgPutFailCount.incrementAndGet();
        } else {
            this.msgPutSize.addAndGet(size);
        }
        long old = this.msgPutTimeMax.get();
        while (!this.msgPutTimeMax.compareAndSet(old, Math.max(old, time))) {
            old = this.msgPutTimeMax.get();
        }
    }

    public HashMap<String, String> runtimeInfo() {
        HashMap<String, String> runtimeInfo = new HashMap<>();

        // statsService
        runtimeInfo.put("bootTimestamp", String.valueOf(this.getBootTimestamp()));
        runtimeInfo.put("runtime", this.getFormatRuntime());
        runtimeInfo.put("putMessageEntireTimeMax", String.valueOf(msgPutTimeMax.get()));
        runtimeInfo.put("putMessageTimesTotal", String.valueOf(msgPutCount.get()));
        runtimeInfo.put("putMessageFailedTimes", String.valueOf(msgGetTimeMax.get()));
        runtimeInfo.put("putMessageSizeTotal", String.valueOf(msgPutSize.get()));
        runtimeInfo.put("putMessageAverageSize", String.valueOf(msgPutSize.get() / msgPutCount.doubleValue()));
        runtimeInfo.put("getMessageEntireTimeMax", String.valueOf(msgGetTimeMax.get()));
        runtimeInfo.put("putTps", String.valueOf(computedPutTPS));
        runtimeInfo.put("getTransferedTps", String.valueOf(computedGetQPS));

        runtimeInfo.put("msgPutTotalYesterdayMorning", String.valueOf(getMsgPutTotalYesterdayMorning()));
        runtimeInfo.put("msgPutTotalTodayMorning", String.valueOf(getMsgPutTotalTodayMorning()));
        runtimeInfo.put("msgPutTotalTodayNow", String.valueOf(getMsgPutTotalTodayNow()));
        runtimeInfo.put("msgGetTotalYesterdayMorning", String.valueOf(getMsgGetTotalYesterdayMorning()));
        runtimeInfo.put("msgGetTotalTodayMorning", String.valueOf(getMsgGetTotalTodayMorning()));
        runtimeInfo.put("msgGetTotalTodayNow", String.valueOf(getMsgGetTotalTodayNow()));
        return runtimeInfo;
    }
}
