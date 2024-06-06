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
package org.apache.rocketmq.proxy.proxy.mapping.proxy;

import com.google.common.collect.Sets;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.proxy.helper.MapBuilder;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.assertj.core.util.Maps;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;

public class ClusterMappingTest {
    private ClusterMapping clusterMapping;

    @Before
    public void setUp() {
        clusterMapping = new ClusterMapping(new NettyClientConfig());
        ClusterInfo brokerInfo = new ClusterInfo();
        brokerInfo.setBrokerAddrTable(
                new MapBuilder<String, BrokerData>()
                        .add("broker-a", new BrokerData("", "broker-a", (HashMap<Long, String>) Maps.newHashMap(0L, "127.0.0.1")))
                        .add("broker-b", new BrokerData("", "broker-b", (HashMap<Long, String>) Maps.newHashMap(0L, "127.0.1.1")))
                        .add("broker-c", new BrokerData("", "broker-c", MapBuilder.builder(0L, "127.0.2.1").add(1L, "127.0.2.2").build()))
                        .build());

        ClusterInfo proxyInfo = new ClusterInfo();
        proxyInfo.setBrokerAddrTable(new MapBuilder<String, BrokerData>().add("proxy-a", new BrokerData("", "proxy-a", (HashMap<Long, String>) Maps.newHashMap(0L, "127.0.0.2"))).add("proxy-b", new BrokerData("", "proxy-b", (HashMap<Long, String>) Maps.newHashMap(0L, "127.0.1.2"))).build());
        clusterMapping.updateBrokerNameAddress(brokerInfo);
        clusterMapping.updateProxyNameAddress(proxyInfo);
    }

    @Test
    public void testGetProxyAddress() {
        assertThat(clusterMapping.getProxyAddress("proxy-a")).isEqualTo(new MapBuilder<>().add(0L, "127.0.0.2").build());
        assertThat(clusterMapping.getProxyAddress("proxy-c")).isNull();
    }

    @Test
    public void testGetProxyName() {
        assertThat(clusterMapping.getProxyName("127.0.0.2")).isEqualTo("proxy-a");
        assertThat(clusterMapping.getProxyName("127.0.0.1")).isNull();
    }

    @Test
    public void testGetProxyNames() {
        assertThat(clusterMapping.getProxyNames()).isEqualTo(Sets.newHashSet("proxy-a", "proxy-b"));
    }

    @Test
    public void testGetBrokerAddress() {
        assertThat(clusterMapping.getBrokerAddress("broker-a")).isEqualTo(new MapBuilder<>().add(0L, "127.0.0.1").build());
        assertThat(clusterMapping.getBrokerAddress("broker-d")).isNull();
    }

    @Test
    public void testGetBrokerName() {
        assertThat(clusterMapping.getBrokerName("127.0.0.1")).isEqualTo("broker-a");
        assertThat(clusterMapping.getBrokerName("127.0.0.3")).isNull();
    }

    @Test
    public void testGetBrokerNames() {
        assertThat(clusterMapping.getBrokerNames()).isEqualTo(Sets.newHashSet("broker-a", "broker-b", "broker-c"));
    }

    @Test
    public void testGetBrokerAddressRemotingClientByAddr() {
        assertThat(clusterMapping.getBrokerAddressRemotingClientByAddr("127.0.0.1")).isNotNull();
        assertThat(clusterMapping.getBrokerAddressRemotingClientByAddr("127.0.1.1")).isNotNull();
        assertThat(clusterMapping.getBrokerAddressRemotingClientByAddr("127.0.0.2")).isNull();
        assertThat(clusterMapping.getBrokerAddressRemotingClientByAddr("127.0.1.2")).isNull();
        assertThat(clusterMapping.getBrokerAddressRemotingClientByAddr("127.0.0.3")).isNull();
    }

    @Test
    public void testUpdateProxyNameAddress() {
        ClusterInfo proxyInfo = new ClusterInfo();
        proxyInfo.setBrokerAddrTable(new MapBuilder<String, BrokerData>().add("proxy-a", new BrokerData("", "proxy-a", (HashMap<Long, String>) Maps.newHashMap(0L, "127.1.0.2"))).add("proxy-b", new BrokerData("", "proxy-b", (HashMap<Long, String>) Maps.newHashMap(0L, "127.1.1.2"))).add("proxy-c", new BrokerData("", "proxy-c", (HashMap<Long, String>) Maps.newHashMap(0L, "127.1.2.2"))).build());
        clusterMapping.updateProxyNameAddress(proxyInfo);

        assertThat(clusterMapping.getProxyName("127.1.0.2")).isEqualTo("proxy-a");
        assertThat(clusterMapping.getProxyName("127.0.0.1")).isNull();
        assertThat(clusterMapping.getProxyNames()).isEqualTo(Sets.newHashSet("proxy-a", "proxy-b", "proxy-c"));

        assertThat(clusterMapping.getProxyAddress("proxy-a")).isEqualTo(new MapBuilder<>().add(0L, "127.1.0.2").build());
        assertThat(clusterMapping.getProxyAddress("proxy-c")).isEqualTo(new MapBuilder<>().add(0L, "127.1.2.2").build());
        assertThat(clusterMapping.getProxyAddress("proxy-d")).isNull();

        // null test
        clusterMapping.updateProxyNameAddress(null);
        assertThat(clusterMapping.getProxyName("127.1.0.2")).isNull();
        assertThat(clusterMapping.getProxyNames()).isEmpty();
        assertThat(clusterMapping.getProxyAddress("proxy-a")).isNull();
    }

    @Test
    public void testUpdateBrokerNameAddress() {
        ClusterInfo brokerInfo = new ClusterInfo();
        brokerInfo.setBrokerAddrTable(new MapBuilder<String, BrokerData>().add("broker-a", new BrokerData("", "broker-a", (HashMap<Long, String>) Maps.newHashMap(0L, "127.1.0.2"))).add("broker-b", new BrokerData("", "broker-b", (HashMap<Long, String>) Maps.newHashMap(0L, "127.1.1.2"))).add("broker-c", new BrokerData("", "broker-c", (HashMap<Long, String>) Maps.newHashMap(0L, "127.1.2.2"))).build());
        clusterMapping.updateBrokerNameAddress(brokerInfo);

        assertThat(clusterMapping.getBrokerName("127.1.0.2")).isEqualTo("broker-a");
        assertThat(clusterMapping.getBrokerName("127.0.0.1")).isNull();
        assertThat(clusterMapping.getBrokerNames()).isEqualTo(Sets.newHashSet("broker-a", "broker-b", "broker-c"));

        assertThat(clusterMapping.getBrokerAddress("broker-a")).isEqualTo(new MapBuilder<>().add(0L, "127.1.0.2").build());
        assertThat(clusterMapping.getBrokerAddress("broker-c")).isEqualTo(new MapBuilder<>().add(0L, "127.1.2.2").build());
        assertThat(clusterMapping.getBrokerAddress("broker-d")).isNull();

        // null test
        clusterMapping.updateBrokerNameAddress(null);
        assertThat(clusterMapping.getBrokerName("127.1.0.2")).isNull();
        assertThat(clusterMapping.getBrokerNames()).isEmpty();
        assertThat(clusterMapping.getBrokerAddress("broker-a")).isNull();
    }

    @Test
    public void selectBrokerAddress() {
        assertThat(clusterMapping.selectBrokerAddress("broker-a")).isEqualTo("127.0.0.1");
        assertThat(clusterMapping.selectBrokerAddress("broker-a", true)).isEqualTo("127.0.0.1");
        assertThat(clusterMapping.selectBrokerAddress("broker-c")).isEqualTo("127.0.2.1");
        assertThat(clusterMapping.selectBrokerAddress("broker-c", true)).isEqualTo("127.0.2.2");
    }
}
