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
package org.apache.rocketmq.proxy.proxy.routeinfo;

import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.proxy.controller.ProxyController;
import org.apache.rocketmq.proxy.proxy.out.ProxyOuterAPI;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ProxyClusterInfoTest {
    private ProxyClusterInfo proxyClusterInfo;
    private final ClusterInfo proxyClusterInfo_ = mock(ClusterInfo.class);
    private final ClusterInfo brokerClusterInfo_ = mock(ClusterInfo.class);

    @Before
    public void setUp() throws Exception {
        ProxyController proxyController = mock(ProxyController.class);
        ProxyOuterAPI proxyOuterAPI = mock(ProxyOuterAPI.class);
        when(proxyOuterAPI.getBrokerClusterInfo()).thenReturn(brokerClusterInfo_);
        when(proxyOuterAPI.getProxyClusterInfo()).thenReturn(proxyClusterInfo_);

        when(proxyController.getProxyOuterAPI()).thenReturn(proxyOuterAPI);
        proxyClusterInfo = new ProxyClusterInfo(proxyController);
    }

    @Test
    public void testGetBrokerClusterInfo() {
        assertThat(proxyClusterInfo.getBrokerClusterInfo()).isNotNull();
    }

    @Test
    public void testGetProxyClusterInfo() {
        assertThat(proxyClusterInfo.getBrokerClusterInfo()).isNotNull();
    }

    @Test
    public void testUpdate() {
        assertThat(proxyClusterInfo.getBrokerClusterInfo()).isNotEqualTo(brokerClusterInfo_);
        assertThat(proxyClusterInfo.getProxyClusterInfo()).isNotEqualTo(proxyClusterInfo_);
        proxyClusterInfo.update();
        assertThat(proxyClusterInfo.getBrokerClusterInfo()).isEqualTo(brokerClusterInfo_);
        assertThat(proxyClusterInfo.getProxyClusterInfo()).isEqualTo(proxyClusterInfo_);
    }
}
