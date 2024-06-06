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
package org.apache.rocketmq.proxy.controller;

import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.proxy.common.config.ProxyConfig;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class ProxyControllerTest {
    private ProxyController proxyController;

    @Before
    public void setup() {
        System.setProperty("rocketmq.namesrv.addr", "127.0.0.1:11111");
        proxyController = new ProxyController(
                new ProxyConfig(),
                new BrokerConfig(),
                new NettyServerConfig(),
                new NettyClientConfig()
        );
    }

    @Test
    public void testStart() {
        proxyController.start();
    }

    @Test
    public void testGetBrokerController() {
        assertNotNull(proxyController.getBrokerController());
    }

    @Test
    public void testGetProxyOuterAPI() {
        assertNotNull(proxyController.getProxyOuterAPI());
    }

    @Test
    public void testGetTopicRouteInfo() {
        assertNotNull(proxyController.getTopicRouteInfo());
    }

    @Test
    public void testGetProxyClusterInfo() {
        assertNotNull(proxyController.getProxyClusterInfo());
    }

    @Test
    public void testGetClusterMapping() {
        assertNotNull(proxyController.getClusterMapping());
    }

    @Test
    public void testGetProxyConfig() {
        assertNotNull(proxyController.getProxyConfig());
    }

    @Test
    public void testGetBrokerConfig() {
        assertNotNull(proxyController.getBrokerConfig());
    }

    @Test
    public void testGetNettyServerConfig() {
        assertNotNull(proxyController.getNettyServerConfig());
    }

    @Test
    public void testGetNettyClientConfig() {
        assertNotNull(proxyController.getNettyClientConfig());
    }
}
