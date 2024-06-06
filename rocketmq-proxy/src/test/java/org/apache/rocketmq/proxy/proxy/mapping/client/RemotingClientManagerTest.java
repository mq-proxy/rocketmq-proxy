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
package org.apache.rocketmq.proxy.proxy.mapping.client;

import com.google.common.collect.Sets;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class RemotingClientManagerTest {

    private RemotingClientManager remotingClientManager;
    private NettyClientConfig nettyClientConfig;

    @Before
    public void setUp() throws Exception {
        nettyClientConfig = new NettyClientConfig();
        remotingClientManager = new RemotingClientManager(nettyClientConfig);
    }

    @Test
    public void getNettyClientConfig() {
        assertThat(remotingClientManager.getNettyClientConfig(ClientType.DEFAULT)).isNotNull();
        assertThat(remotingClientManager.getNettyClientConfig(ClientType.CONSUMER)).isEqualTo(nettyClientConfig);
        assertThat(remotingClientManager.getNettyClientConfig(ClientType.PRODUCER)).isEqualTo(nettyClientConfig);
        assertThat(remotingClientManager.getNettyClientConfig(ClientType.ADMIN)).isEqualTo(nettyClientConfig);
    }

    @Test
    public void setNettyClientConfig() {
        NettyClientConfig newClientConfig = new NettyClientConfig();
        remotingClientManager.setNettyClientConfig(ClientType.CONSUMER, newClientConfig);
        assertThat(remotingClientManager.getNettyClientConfig(ClientType.CONSUMER)).isNotEqualTo(nettyClientConfig);
        assertThat(remotingClientManager.getNettyClientConfig(ClientType.CONSUMER)).isEqualTo(newClientConfig);
        assertThat(remotingClientManager.getNettyClientConfig(ClientType.PRODUCER)).isEqualTo(nettyClientConfig);
    }

    @Test
    public void getRemotingClient() {
        assertThat(remotingClientManager.getRemotingClient("test", ClientType.CONSUMER)).isNull();
        remotingClientManager.createRemotingClients("test");
        assertThat(remotingClientManager.getRemotingClient("test", ClientType.DEFAULT)).isNotNull();
        assertThat(remotingClientManager.getRemotingClient("test", ClientType.PRODUCER)).isNotNull();
        assertThat(remotingClientManager.getRemotingClient("test", ClientType.CONSUMER)).isNotNull();
    }

    @Test
    public void applyRemotingClients() {
        Set<String> addrSet = new HashSet<>();
        remotingClientManager.applyRemotingClients(ClientType.DEFAULT, (addr, client) -> addrSet.add(addr));
        assertThat(addrSet).isEqualTo(Sets.newHashSet());

        addrSet.clear();
        remotingClientManager.createRemotingClients("test");
        remotingClientManager.applyRemotingClients(ClientType.DEFAULT, (addr, client) -> addrSet.add(addr));
        assertThat(addrSet).isEqualTo(Sets.newHashSet("test"));

        addrSet.clear();
        remotingClientManager.createRemotingClients("test2");
        remotingClientManager.applyRemotingClients(ClientType.DEFAULT, (addr, client) -> addrSet.add(addr));
        assertThat(addrSet).isEqualTo(Sets.newHashSet("test", "test2"));
    }

    @Test
    public void createRemotingClients() {
        assertThat(remotingClientManager.getRemotingClient("test", ClientType.CONSUMER)).isNull();
        remotingClientManager.createRemotingClients("test");
        assertThat(remotingClientManager.getRemotingClient("test", ClientType.DEFAULT)).isNotNull();
    }

    @Test
    public void destroyRemotingClients() {
        remotingClientManager.createRemotingClients("test");
        assertThat(remotingClientManager.getRemotingClient("test", ClientType.DEFAULT)).isNotNull();
        remotingClientManager.destroyRemotingClients("test");
        assertThat(remotingClientManager.getRemotingClient("test", ClientType.CONSUMER)).isNull();
    }

    @Test
    public void clearUnusedRemotingClients() {
        remotingClientManager.createRemotingClients("test");
        remotingClientManager.createRemotingClients("test2");
        assertThat(remotingClientManager.getRemotingClient("test", ClientType.DEFAULT)).isNotNull();
        assertThat(remotingClientManager.getRemotingClient("test2", ClientType.DEFAULT)).isNotNull();

        remotingClientManager.clearUnusedRemotingClients(Sets.newHashSet("test"));
        assertThat(remotingClientManager.getRemotingClient("test", ClientType.DEFAULT)).isNotNull();
        assertThat(remotingClientManager.getRemotingClient("test2", ClientType.DEFAULT)).isNull();

        remotingClientManager.createRemotingClients("test3");
        remotingClientManager.clearUnusedRemotingClients(null);
        assertThat(remotingClientManager.getRemotingClient("test", ClientType.CONSUMER)).isNull();
        assertThat(remotingClientManager.getRemotingClient("test3", ClientType.CONSUMER)).isNull();
    }

    @Test
    public void testFinalize() {
        remotingClientManager.finalize();
    }
}