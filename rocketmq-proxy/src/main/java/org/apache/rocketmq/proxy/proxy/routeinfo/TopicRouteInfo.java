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

import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.common.protocol.header.namesrv.DeleteTopicFromNamesrvRequestHeader;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.proxy.common.log.LoggerName;
import org.apache.rocketmq.proxy.common.protocol.TopicRouteTable;
import org.apache.rocketmq.proxy.controller.ProxyController;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class TopicRouteInfo {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    private final ProxyController proxyController;
    private final ConcurrentMap<String, TopicRouteData> topicRouteInfoMap = new ConcurrentHashMap<>();
    private final TopicList topicList = new TopicList();

    public TopicRouteInfo(ProxyController proxyController) {
        this.proxyController = proxyController;
        this.topicList.setTopicList(new HashSet<>());
    }

    public TopicRouteData put(String topic, TopicRouteData topicRouteData) {
        if (topic == null || topicRouteData == null) {
            return null;
        }
        return this.topicRouteInfoMap.put(topic, topicRouteData);
    }

    public TopicRouteData get(String topic) {
        return this.topicRouteInfoMap.get(topic);
    }

    public TopicRouteData remove(String topic) {
        return this.topicRouteInfoMap.remove(topic);
    }

    public TopicList getTopicList() {
        return this.topicList;
    }

    // return a copy
    public Map<String, TopicRouteData> getAll() {
        return new HashMap<>(this.topicRouteInfoMap);
    }

    public synchronized void cleanUnused(TopicList topicList) {
        cleanUnused(topicList == null ? null : topicList.getTopicList());
    }

    public synchronized void cleanUnused(Set<String> topics) {
        if (topics == null || topics.isEmpty()) {
            Set<String> topicSet = new HashSet<>(this.topicRouteInfoMap.keySet());
            this.topicRouteInfoMap.clear();
            notifyTopicDeleted(topicSet);
            return;
        }

        Set<String> deletingTopics = this.topicRouteInfoMap.keySet().stream()
                .filter(s -> !topics.contains(s)).collect(Collectors.toSet());
        if (deletingTopics.isEmpty()) {
            return;
        }

        this.topicRouteInfoMap.keySet().removeIf(deletingTopics::contains);
        notifyTopicDeleted(deletingTopics);
    }

    // update topic route info from namesrv
    public synchronized void update() {
        try {
            TopicList topicList = proxyController.getProxyOuterAPI().getBrokerTopicList();
            // clear invalid topic
            cleanUnused(topicList);
            this.topicList.setTopicList(topicList.getTopicList());
            TopicRouteTable routeData = proxyController.getProxyOuterAPI().getBrokerTopicRoute();
            if (routeData == null || routeData.getTopicRouteData() == null || routeData.getTopicRouteData().isEmpty()) {
                cleanUnused(new HashSet<>());
                return;
            }
            this.topicRouteInfoMap.putAll(routeData.getTopicRouteData());
        } catch (Exception e) {
            log.warn("update topic route info failed", e);
        }
    }

    private synchronized void notifyTopicDeleted(Set<String> topics) {
        if (topics == null || topics.isEmpty()) {
            return;
        }

        String nameservers = this.proxyController.getBrokerConfig().getNamesrvAddr();
        if (nameservers == null || nameservers.isEmpty()) {
            return;
        }

        String[] nameserverArr = nameservers.split(";");

        for (String topic : topics) {
            DeleteTopicFromNamesrvRequestHeader header = new DeleteTopicFromNamesrvRequestHeader();
            header.setTopic(topic);
            RemotingCommand request = RemotingCommand.createRequestCommand(
                    RequestCode.DELETE_TOPIC_IN_NAMESRV,
                    header
            );

            for (String namesrvAddr : nameserverArr) {
                try {
                    RemotingHelper.invokeSync(namesrvAddr, request,  5000);
                } catch (InterruptedException | RemotingConnectException | RemotingSendRequestException |
                         RemotingTimeoutException | RemotingCommandException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
