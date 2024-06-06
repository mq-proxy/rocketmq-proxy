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

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.proxy.broker.BrokerController;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.proxy.broker.processor.admin.ClientProcessor;
import org.apache.rocketmq.proxy.broker.processor.admin.ConfigProcessor;
import org.apache.rocketmq.proxy.broker.processor.admin.ConsumerProcessor;
import org.apache.rocketmq.proxy.broker.processor.admin.OffsetProcessor;
import org.apache.rocketmq.proxy.broker.processor.admin.StatProcessor;
import org.apache.rocketmq.proxy.broker.processor.admin.SubscriptionGroupProcessor;
import org.apache.rocketmq.proxy.broker.processor.admin.TopicProcessor;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.HashMap;
import java.util.Map;

public class AdminBrokerProcessor extends AbstractReplyProcessor implements DefaultRequestProcessor {
    private final Map<Integer, DefaultRequestProcessor> processors = new HashMap<>();

    // proxy to broker
    private final DefaultRequestProcessor defaultBrokerRequestProcessor = this::doProxy;

    private final TopicProcessor topicProcessor;
    private final OffsetProcessor offsetProcessor;
    private final ConfigProcessor configProcessor;
    private final SubscriptionGroupProcessor subscriptionGroupProcessor;
    private final ClientProcessor clientProcessor;
    private final ConsumerProcessor consumerProcessor;
    private final StatProcessor statProcessor;

    public AdminBrokerProcessor(final BrokerController brokerController) {
        super(brokerController);
        topicProcessor = new TopicProcessor(brokerController);
        offsetProcessor = new OffsetProcessor(brokerController);
        configProcessor = new ConfigProcessor(brokerController);
        subscriptionGroupProcessor = new SubscriptionGroupProcessor(brokerController);
        clientProcessor = new ClientProcessor(brokerController);
        consumerProcessor = new ConsumerProcessor(brokerController);
        statProcessor = new StatProcessor(brokerController);
        init();
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        DefaultRequestProcessor processor = processors.get(request.getCode());
        if (processor == null) {
            return getUnknownCmdResponse(ctx, request);
        } else {
            try {
                return processor.processRequest(ctx, request);
            } catch (Exception e) {
                throw new RemotingCommandException(e.getMessage(), e);
            }
        }
    }

    private void init() {
        processors.put(RequestCode.UPDATE_AND_CREATE_TOPIC, topicProcessor);
        processors.put(RequestCode.DELETE_TOPIC_IN_BROKER, topicProcessor);
        processors.put(RequestCode.GET_ALL_TOPIC_CONFIG, topicProcessor);

        processors.put(RequestCode.UPDATE_BROKER_CONFIG, configProcessor);
        processors.put(RequestCode.GET_BROKER_CONFIG, configProcessor);

        processors.put(RequestCode.SEARCH_OFFSET_BY_TIMESTAMP, offsetProcessor);
        processors.put(RequestCode.GET_MAX_OFFSET, offsetProcessor);
        processors.put(RequestCode.GET_MIN_OFFSET, offsetProcessor);

        processors.put(RequestCode.GET_EARLIEST_MSG_STORETIME, statProcessor);

        processors.put(RequestCode.GET_BROKER_RUNTIME_INFO, statProcessor);
        processors.put(RequestCode.LOCK_BATCH_MQ, consumerProcessor);
        processors.put(RequestCode.UNLOCK_BATCH_MQ, consumerProcessor);

        processors.put(RequestCode.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP, subscriptionGroupProcessor);
        processors.put(RequestCode.GET_ALL_SUBSCRIPTIONGROUP_CONFIG, subscriptionGroupProcessor);
        processors.put(RequestCode.DELETE_SUBSCRIPTIONGROUP, subscriptionGroupProcessor);

        processors.put(RequestCode.GET_TOPIC_STATS_INFO, statProcessor);

        processors.put(RequestCode.GET_CONSUMER_CONNECTION_LIST, clientProcessor);
        processors.put(RequestCode.GET_PRODUCER_CONNECTION_LIST, clientProcessor);
        processors.put(RequestCode.GET_ALL_PRODUCER_INFO, clientProcessor);
        processors.put(RequestCode.GET_CONSUME_STATS, statProcessor);

        processors.put(RequestCode.GET_ALL_CONSUMER_OFFSET, offsetProcessor);
        processors.put(RequestCode.GET_ALL_DELAY_OFFSET, offsetProcessor);
        processors.put(RequestCode.INVOKE_BROKER_TO_RESET_OFFSET, offsetProcessor);

        processors.put(RequestCode.INVOKE_BROKER_TO_GET_CONSUMER_STATUS, statProcessor);

        processors.put(RequestCode.QUERY_TOPIC_CONSUME_BY_WHO, consumerProcessor);
        processors.put(RequestCode.QUERY_CONSUME_TIME_SPAN, consumerProcessor);

        processors.put(RequestCode.GET_SYSTEM_TOPIC_LIST_FROM_BROKER, topicProcessor);

        processors.put(RequestCode.CLEAN_EXPIRED_CONSUMEQUEUE, defaultBrokerRequestProcessor);
        processors.put(RequestCode.DELETE_EXPIRED_COMMITLOG, defaultBrokerRequestProcessor);
        processors.put(RequestCode.CLEAN_UNUSED_TOPIC, defaultBrokerRequestProcessor);

        // broker&client
        processors.put(RequestCode.GET_CONSUMER_RUNNING_INFO, consumerProcessor);

        processors.put(RequestCode.QUERY_CORRECTION_OFFSET, offsetProcessor);
        processors.put(RequestCode.CONSUME_MESSAGE_DIRECTLY, consumerProcessor);
        processors.put(RequestCode.CLONE_GROUP_OFFSET, offsetProcessor);
        processors.put(RequestCode.VIEW_BROKER_STATS_DATA, statProcessor);
        processors.put(RequestCode.GET_BROKER_CONSUME_STATS, consumerProcessor);
        processors.put(RequestCode.QUERY_CONSUME_QUEUE, consumerProcessor);

        processors.put(RequestCode.UPDATE_AND_CREATE_ACL_CONFIG, configProcessor);
        processors.put(RequestCode.DELETE_ACL_CONFIG, configProcessor);
        processors.put(RequestCode.GET_BROKER_CLUSTER_ACL_INFO, configProcessor);
        processors.put(RequestCode.UPDATE_GLOBAL_WHITE_ADDRS_CONFIG, configProcessor);
        processors.put(RequestCode.GET_BROKER_CLUSTER_ACL_CONFIG, configProcessor);

        processors.put(RequestCode.PUSH_REPLY_MESSAGE_TO_CLIENT, this::replyMessage);
    }
}
