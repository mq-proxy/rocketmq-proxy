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
package org.apache.rocketmq.proxy.proxy;

import com.google.common.collect.Sets;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.proxy.broker.BrokerController;
import org.apache.rocketmq.proxy.controller.Controllers;
import org.apache.rocketmq.proxy.controller.ProxyController;
import org.apache.rocketmq.proxy.helper.MapBuilder;
import org.apache.rocketmq.proxy.proxy.mapping.proxy.ClusterMapping;
import org.apache.rocketmq.proxy.proxy.routeinfo.ProxyClusterInfo;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class ProxyTest {

    private final String PROXY_NAME = "broker-a";
    private final String BROKER_ADDR_MASTER = "127.0.0.1:10911";
    private final String BROKER_ADDR_SLAVE = "127.0.0.1:10912";

    private final String BROKER_ADDR_MASTER2 = "127.0.0.2:10911";
    private final String BROKER_ADDR_SLAVE2 = "127.0.0.2:10912";

    @Before
    public void setUp() {

    }

    @Test
    public void testProxyId() {
        assertThat(Proxy.getProxyId()).isNotEmpty();
    }

    @Test
    public void testProxyName() {
        try (MockedStatic<Controllers> controllersMockedStatic = mockStatic(Controllers.class)) {
            ProxyController proxyController = nullQueueMock();
            controllersMockedStatic.when(Controllers::getController).thenReturn(proxyController);
            assertThat(Proxy.getProxyName()).isEqualTo(PROXY_NAME);
        }
    }

    @Test
    public void testGetBrokerAddr() {
        try (MockedStatic<Controllers> controllersMockedStatic = mockStatic(Controllers.class)) {
            ProxyController proxyController = normalMock();
            controllersMockedStatic.when(Controllers::getController).thenReturn(proxyController);
            String addr = Proxy.getBrokerAddr(true);
            assertThat(addr).isEqualTo(BROKER_ADDR_SLAVE);
            addr = Proxy.getBrokerAddr(false);
            assertThat(addr).isEqualTo(BROKER_ADDR_MASTER);
        }
    }

    @Test
    public void testGetMasterBrokerAddr() {
        try (MockedStatic<Controllers> controllersMockedStatic = mockStatic(Controllers.class)) {
            ProxyController proxyController = normalMock();
            controllersMockedStatic.when(Controllers::getController).thenReturn(proxyController);
            String addr = Proxy.getMasterBrokerAddr();
            assertThat(addr).isEqualTo(BROKER_ADDR_MASTER);
        }
    }

    @Test
    public void testGetAllBrokerAddresses() {
        try (MockedStatic<Controllers> controllersMockedStatic = mockStatic(Controllers.class)) {
            ProxyController proxyController = normalMock();
            controllersMockedStatic.when(Controllers::getController).thenReturn(proxyController);
            Set<String> addr = Proxy.getAllBrokerAddresses();
            assertThat(addr).isEqualTo(Sets.newHashSet(BROKER_ADDR_MASTER, BROKER_ADDR_SLAVE));
        }
    }

    @Test
    public void testGetClusters() {
        try (MockedStatic<Controllers> controllersMockedStatic = mockStatic(Controllers.class)) {
            ProxyController proxyController = mockClusterInfo(null, null);
            controllersMockedStatic.when(Controllers::getController).thenReturn(proxyController);
            assertThat(Proxy.getAllClusters()).isNull();
        }

        try (MockedStatic<Controllers> controllersMockedStatic = mockStatic(Controllers.class)) {
            BrokerData brokerData = new BrokerData();
            brokerData.setBrokerName("BrokerA");
            brokerData.setCluster("DefaultCluster");
            brokerData.setBrokerAddrs(new MapBuilder<Long, String>()
                    .add(0L, BROKER_ADDR_MASTER)
                    .add(1L, BROKER_ADDR_SLAVE)
                    .build());

            ProxyController proxyController = mockClusterInfo(Collections.singletonList(brokerData), null);
            controllersMockedStatic.when(Controllers::getController).thenReturn(proxyController);
            assertThat(Proxy.getAllClusters()).isEqualTo(Sets.newHashSet("DefaultCluster"));
        }

        try (MockedStatic<Controllers> controllersMockedStatic = mockStatic(Controllers.class)) {
            BrokerData brokerData = new BrokerData();
            brokerData.setBrokerName("BrokerA");
            brokerData.setCluster("DefaultCluster");

            brokerData.setBrokerAddrs(new MapBuilder<Long, String>()
                    .add(0L, BROKER_ADDR_MASTER)
                    .add(1L, BROKER_ADDR_SLAVE)
                    .build());

            BrokerData brokerData2 = new BrokerData();
            brokerData2.setBrokerName("BrokerB");
            brokerData2.setCluster("DefaultCluster");

            brokerData2.setBrokerAddrs(new MapBuilder<Long, String>()
                    .add(0L, BROKER_ADDR_MASTER2)
                    .add(1L, BROKER_ADDR_SLAVE2)
                    .build());

            BrokerData brokerData3 = new BrokerData();
            brokerData3.setBrokerName("BrokerC");
            brokerData3.setCluster("DefaultCluster2");

            brokerData3.setBrokerAddrs(new MapBuilder<Long, String>()
                    .add(0L, BROKER_ADDR_MASTER2)
                    .add(1L, BROKER_ADDR_SLAVE2)
                    .build());

            ProxyController proxyController = mockClusterInfo(Arrays.asList(brokerData, brokerData2, brokerData3), null);
            controllersMockedStatic.when(Controllers::getController).thenReturn(proxyController);
            assertThat(Proxy.getAllClusters()).isEqualTo(Sets.newHashSet("DefaultCluster", "DefaultCluster2"));
        }
    }

    private ProxyController nullQueueMock() {
        return doMock(null);
    }

    private ProxyController normalMock() {
        BrokerData brokerData = new BrokerData();
        brokerData.setBrokerName(PROXY_NAME);
        brokerData.setBrokerAddrs(new MapBuilder<Long, String>()
                .add(0L, BROKER_ADDR_MASTER)
                .add(1L, BROKER_ADDR_SLAVE).build());

        return doMock(Collections.singletonList(brokerData));
    }

    private ProxyController mockClusterInfo(List<BrokerData> brokerData, List<BrokerData> proxyData) {
        ClusterInfo broker = genClusterInfo(brokerData);
        ClusterInfo proxy = genClusterInfo(proxyData);
        ProxyController proxyController = mock(ProxyController.class);
        ProxyClusterInfo proxyClusterInfo = mock(ProxyClusterInfo.class);
        when(proxyController.getProxyClusterInfo()).thenReturn(proxyClusterInfo);
        when(proxyClusterInfo.getBrokerClusterInfo()).thenReturn(broker);
        when(proxyClusterInfo.getProxyClusterInfo()).thenReturn(proxy);
        return proxyController;
    }

    private ClusterInfo genClusterInfo(List<BrokerData> brokerDataList) {
        ClusterInfo clusterInfo = null;
        if (brokerDataList != null && !brokerDataList.isEmpty()) {
            clusterInfo = new ClusterInfo();
            HashMap<String, BrokerData> brokerDataHashMap = new HashMap<>();
            HashMap<String, Set<String>> clusterMap = new HashMap<>();
            brokerDataList.forEach(brokerData -> {
                brokerDataHashMap.put(brokerData.getBrokerName(), brokerData);
                clusterMap.computeIfAbsent(brokerData.getCluster(), k -> Sets.newHashSet()).add(brokerData.getBrokerName());
            });
            clusterInfo.setBrokerAddrTable(brokerDataHashMap);
            clusterInfo.setClusterAddrTable(clusterMap);
        }
        return clusterInfo;
    }

    private ClusterInfo genClusterInfo(Set<String> names) {
        ClusterInfo clusterInfo = null;
        if (names != null && !names.isEmpty()) {
            clusterInfo = new ClusterInfo();
            HashMap<String, BrokerData> brokerDataHashMap = new HashMap<>();
            HashMap<String, Set<String>> clusterMap = new HashMap<>();
            names.forEach(name -> {
                BrokerData brokerData = new BrokerData();
                brokerData.setBrokerName(name);
                brokerData.setCluster("DefaultCluster");
                brokerData.setBrokerAddrs(MapBuilder.builder(0L, "127.0.0.1:19876").build());
                brokerDataHashMap.put(name, brokerData);
                clusterMap.computeIfAbsent(brokerData.getCluster(), k -> Sets.newHashSet()).add(brokerData.getBrokerName());
            });
            clusterInfo.setBrokerAddrTable(brokerDataHashMap);
            clusterInfo.setClusterAddrTable(clusterMap);
        }
        return clusterInfo;
    }

    private ProxyController doMock(List<BrokerData> brokerData) {
        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setBrokerName(PROXY_NAME);

        BrokerController brokerController = mock(BrokerController.class);
        when(brokerController.getBrokerConfig()).thenReturn(brokerConfig);
        ProxyController proxyController = mock(ProxyController.class);
        when(proxyController.getBrokerController()).thenReturn(brokerController);
        when(proxyController.getBrokerConfig()).thenReturn(brokerConfig);

        if (brokerData == null) {
            return proxyController;
        }

        ProxyClusterInfo proxyClusterInfo = mock(ProxyClusterInfo.class);
        when(proxyClusterInfo.getBrokerClusterInfo()).thenReturn(genClusterInfo(brokerData));

        when(proxyController.getBrokerController()).thenReturn(brokerController);
        when(proxyController.getProxyClusterInfo()).thenReturn(proxyClusterInfo);

        ClusterMapping clusterMapping = mock(ClusterMapping.class);
        Set<String> proxies = brokerData.stream().map(BrokerData::getBrokerName).collect(Collectors.toSet());
        when(clusterMapping.getProxyNames()).thenReturn(proxies);
        when(clusterMapping.getBrokerAddress(anyString())).thenReturn(
                genClusterInfo(brokerData).getBrokerAddrTable().get(PROXY_NAME).getBrokerAddrs()
        );
        when(proxyController.getClusterMapping()).thenReturn(clusterMapping);
        when(proxyClusterInfo.getProxyClusterInfo()).thenReturn(genClusterInfo(proxies));
        return proxyController;
    }
}
