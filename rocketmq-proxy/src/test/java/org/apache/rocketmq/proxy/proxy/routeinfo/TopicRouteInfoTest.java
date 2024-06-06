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

import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.proxy.common.protocol.TopicRouteTable;
import org.apache.rocketmq.proxy.controller.ProxyController;
import org.apache.rocketmq.proxy.helper.MapBuilder;
import org.apache.rocketmq.proxy.proxy.out.ProxyOuterAPI;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.util.HashMap;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class TopicRouteInfoTest {
    private TopicRouteInfo topicRouteInfo;

    private TopicRouteTable topicRouteData;

    @Before
    public void setUp() {
        TopicList topicList = new TopicList();
        topicList.setTopicList(Sets.newHashSet("test1", "test2"));

        topicRouteData = spy(TopicRouteTable.class);
        topicRouteData.setTopicRouteData(
                MapBuilder.builder("test1", new TopicRouteData())
                        .add("test2", new TopicRouteData()).build()
        );

        ProxyController proxyController = mock(ProxyController.class);
        ProxyOuterAPI proxyOuterAPI = mock(ProxyOuterAPI.class);
        when(proxyController.getProxyOuterAPI()).thenReturn(proxyOuterAPI);
        try {
            when(proxyOuterAPI.getBrokerTopicList()).thenReturn(topicList);
            when(proxyOuterAPI.getBrokerTopicRoute()).thenReturn(topicRouteData);
        } catch (RemotingConnectException | RemotingSendRequestException | RemotingTimeoutException |
                 InterruptedException e) {
            throw new RuntimeException(e);
        }

        when(proxyController.getBrokerConfig()).thenReturn(new BrokerConfig());
        topicRouteInfo = new TopicRouteInfo(proxyController);
    }

    @Test
    public void testPut() {
        TopicRouteData topicRouteData = new TopicRouteData();
        assertThat(topicRouteInfo.put("Test", topicRouteData)).isNull();
        assertThat(topicRouteInfo.put("Test", new TopicRouteData())).isEqualTo(topicRouteData);
        assertThat(topicRouteInfo.put("Test", null)).isNull();
    }

    @Test
    public void testGet() {
        TopicRouteData topicRouteData = new TopicRouteData();
        assertThat(topicRouteInfo.get("Test")).isNull();
        assertThat(topicRouteInfo.put("Test", topicRouteData)).isNull();
        assertThat(topicRouteInfo.get("Test")).isEqualTo(topicRouteData);
    }

    @Test
    public void testGetTopicList() {
        assertThat(topicRouteInfo.getTopicList().getTopicList()).isEmpty();
        topicRouteInfo.update();
        assertThat(topicRouteInfo.getTopicList().getTopicList()).isEqualTo(Sets.newHashSet("test1", "test2"));
    }

    @Test
    public void testGetAll() {
        assertThat(topicRouteInfo.getAll()).isEmpty();
        topicRouteInfo.update();
        assertThat(topicRouteInfo.getAll()).isEqualTo(new MapBuilder<>()
                .add("test1", topicRouteData.getTopicRouteData().get("test1"))
                .add("test2", topicRouteData.getTopicRouteData().get("test2"))
                .build()
        );
    }

    @Test
    public void testCleanUnused() {
        topicRouteInfo.update();
        assertThat(topicRouteInfo.getAll()).isEqualTo(new MapBuilder<>()
                .add("test1", topicRouteData.getTopicRouteData().get("test1"))
                .add("test2", topicRouteData.getTopicRouteData().get("test2"))
                .build()
        );
        TopicList topicList = new TopicList();
        topicList.setTopicList(Sets.newHashSet("test2"));
        try (MockedStatic<RemotingHelper> mockedRemotingHelper = mockStatic(RemotingHelper.class)) {
            mockedRemotingHelper.when(() ->
                RemotingHelper.invokeSync(anyString(), any(), anyLong())
            ).thenReturn(null);
            topicRouteInfo.cleanUnused(topicList);
            assertThat(topicRouteInfo.getAll()).isEqualTo(new MapBuilder<>()
                    .add("test2", topicRouteData.getTopicRouteData().get("test2"))
                    .build()
            );

            topicRouteInfo.cleanUnused((TopicList) null);
            assertThat(topicRouteInfo.getAll()).isEqualTo(new HashMap<>());
        }
    }

    @Test
    public void testCleanUnusedSet() {
        topicRouteInfo.update();
        assertThat(topicRouteInfo.getAll()).isEqualTo(topicRouteData.getTopicRouteData());

        try (MockedStatic<RemotingHelper> mockedRemotingHelper = mockStatic(RemotingHelper.class)) {
            mockedRemotingHelper.when(() ->
                    RemotingHelper.invokeSync(anyString(), any(), anyLong())
            ).thenReturn(null);
            topicRouteInfo.cleanUnused(Sets.newHashSet("test1"));
            assertThat(topicRouteInfo.getAll()).isEqualTo(new MapBuilder<>()
                    .add("test1", topicRouteData.getTopicRouteData().get("test1"))
                    .build()
            );

            topicRouteInfo.cleanUnused((Set<String>) null);
            assertThat(topicRouteInfo.getAll()).isEqualTo(new MapBuilder<>()
                    .build()
            );
        }
    }

    @Test
    public void testUpdate() {
        assertThat(topicRouteInfo.getAll()).isEmpty();
        topicRouteInfo.update();
        assertThat(topicRouteInfo.getAll()).isEqualTo(new MapBuilder<>()
                .add("test1", topicRouteData.getTopicRouteData().get("test1"))
                .add("test2", topicRouteData.getTopicRouteData().get("test2"))
                .build()
        );
    }
}
