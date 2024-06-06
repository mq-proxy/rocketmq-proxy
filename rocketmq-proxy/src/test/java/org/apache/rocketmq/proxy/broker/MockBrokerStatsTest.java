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

import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class MockBrokerStatsTest {

    private MockBrokerStats mockBrokerStats;

    @Before
    public void setUp() throws Exception {
        mockBrokerStats = new MockBrokerStats();
    }

    @Test
    public void getBootTimestamp() {
        assertThat(mockBrokerStats.getBootTimestamp()).isBetween(0L, System.currentTimeMillis());
    }

    @Test
    public void getFormatRuntime() {
        assertThat(mockBrokerStats.getFormatRuntime()).isNotEmpty();
    }

    @Test
    public void recordOneDay() {
        mockBrokerStats.recordPut(1,1,true);
        assertThat(mockBrokerStats.getMsgPutTotalTodayNow() - mockBrokerStats.getMsgPutTotalTodayMorning()).isEqualTo(1);
        mockBrokerStats.recordOneDay();
        assertThat(mockBrokerStats.getMsgPutTotalTodayNow() - mockBrokerStats.getMsgPutTotalTodayMorning()).isEqualTo(0);
    }

    @Test
    public void recordOneSecond() {
        assertThat(Double.parseDouble(mockBrokerStats.runtimeInfo().get("putTps"))).isEqualTo(0);
        mockBrokerStats.recordPut(1, 5, true);
        mockBrokerStats.recordOneSecond();
        assertThat(Double.parseDouble(mockBrokerStats.runtimeInfo().get("putTps"))).isGreaterThan(0);
    }

    @Test
    public void getMsgPutTotalYesterdayMorning() {
        assertThat(mockBrokerStats.getMsgPutTotalYesterdayMorning()).isEqualTo(0);
        mockBrokerStats.recordPut(1, 5, true);
        mockBrokerStats.recordOneDay();
        assertThat(mockBrokerStats.getMsgPutTotalYesterdayMorning()).isEqualTo(0);
        mockBrokerStats.recordOneDay();
        assertThat(mockBrokerStats.getMsgPutTotalYesterdayMorning()).isGreaterThan(0);
    }

    @Test
    public void getMsgPutTotalTodayMorning() {
        assertThat(mockBrokerStats.getMsgPutTotalTodayMorning()).isEqualTo(0);
        mockBrokerStats.recordPut(1, 5, true);
        mockBrokerStats.recordOneDay();
        assertThat(mockBrokerStats.getMsgPutTotalTodayMorning()).isGreaterThan(0);
    }

    @Test
    public void getMsgGetTotalYesterdayMorning() {
        assertThat(mockBrokerStats.getMsgGetTotalYesterdayMorning()).isEqualTo(0);
        mockBrokerStats.recordGet(1, 5, true);
        mockBrokerStats.recordOneDay();
        assertThat(mockBrokerStats.getMsgGetTotalYesterdayMorning()).isEqualTo(0);
        mockBrokerStats.recordOneDay();
        assertThat(mockBrokerStats.getMsgGetTotalYesterdayMorning()).isGreaterThan(0);
    }

    @Test
    public void getMsgGetTotalTodayMorning() {
        assertThat(mockBrokerStats.getMsgGetTotalTodayMorning()).isEqualTo(0);
        mockBrokerStats.recordGet(1, 5, true);
        mockBrokerStats.recordOneDay();
        assertThat(mockBrokerStats.getMsgGetTotalTodayMorning()).isGreaterThan(0);
    }

    @Test
    public void getMsgPutTotalTodayNow() {
        assertThat(mockBrokerStats.getMsgPutTotalTodayNow()).isEqualTo(0);
        mockBrokerStats.recordPut(1, 5, true);
        assertThat(mockBrokerStats.getMsgPutTotalTodayNow()).isGreaterThan(0);
    }

    @Test
    public void getMsgGetTotalTodayNow() {
        assertThat(mockBrokerStats.getMsgGetTotalTodayNow()).isEqualTo(0);
        mockBrokerStats.recordGet(1, 5, true);
        assertThat(mockBrokerStats.getMsgGetTotalTodayNow()).isGreaterThan(0);
    }

    @Test
    public void runtimeInfo() {
        assertThat(mockBrokerStats.runtimeInfo()).isNotEmpty();
    }
}