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
package org.apache.rocketmq.proxy.broker.processor;

import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.proxy.controller.ProxyController;
import org.apache.rocketmq.proxy.helper.MapBuilder;
import org.apache.rocketmq.proxy.proxy.mapping.proxy.ClusterMapping;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Spy;

import javax.management.Query;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import static javax.management.Query.in;
import static org.apache.rocketmq.proxy.helper.FieldHelper.setField;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

public abstract class AdminProcessTestBase extends ProcessorTestBase {
    protected final String brokerName = "broker-a";
    protected final String proxyAddr = "127.0.0.1:19876";
    protected final String proxyName = brokerName;
    protected final String topic = "test-topic";

    @Spy
    protected ClusterMapping clusterMapping = new ClusterMapping(this.brokerController.getNettyClientConfig());

    @Mock
    protected NettyRemotingClient remotingClient;

    @Test
    public void unknownRequest() throws Exception {
        RemotingCommand response = processRequest(RemotingCommand.createRequestCommand(1, null));
        assertThat(response.getCode()).isEqualTo(ResponseCode.REQUEST_CODE_NOT_SUPPORTED);
    }

    protected void doMockTopicQueueMapping() {
    }

    protected void doMockClusterMapping(RemotingCommand rsp) throws Exception {
        // mock clusterMapping
        ClusterInfo brokerClusterInfo = new ClusterInfo();
        ClusterInfo proxyClusterInfo = new ClusterInfo();
        BrokerData brokerData = new BrokerData(
                clusterName,
                brokerName,
                MapBuilder.builder(0L, brokerAddr).build()
        );
        BrokerData proxyData = new BrokerData(
                clusterName,
                brokerName,
                MapBuilder.builder(0L, proxyAddr).build()
        );
        brokerClusterInfo.setBrokerAddrTable(MapBuilder.builder(brokerName, brokerData).build());
        proxyClusterInfo.setBrokerAddrTable(MapBuilder.builder(brokerName, proxyData).build());
        clusterMapping.updateBrokerNameAddress(brokerClusterInfo);
        clusterMapping.updateProxyNameAddress(proxyClusterInfo);

        when(remotingClient.invokeSync(eq(brokerAddr), any(), anyLong())).thenReturn(rsp);
        when(remotingClient.invokeSync(eq(brokerAddrSlave), any(), anyLong())).thenReturn(rsp);
        doReturn(remotingClient).when(clusterMapping).getBrokerAddressRemotingClientByAddr(anyString(), any());

        setField(ProxyController.class, "clusterMapping",
                this.brokerController.getProxyController(), clusterMapping);
    }

    protected byte[] createMessage() throws Exception {
        MessageExt messageExt = new MessageExt();
        messageExt.setMsgId("1");
        messageExt.setTopic("topic");
        messageExt.setTags("tags");
        messageExt.setKeys("keys");
        messageExt.setBornHost(new InetSocketAddress("127.0.0.1", 8080));
        messageExt.setStoreHost(new InetSocketAddress("127.0.0.1", 10911));
        messageExt.setBody("hello world".getBytes(StandardCharsets.UTF_8));
        messageExt.getProperty("a");
        messageExt.getProperties().put("UNIQ_KEY", "1");
        return MessageDecoder.encode(messageExt, false);
    }
}
