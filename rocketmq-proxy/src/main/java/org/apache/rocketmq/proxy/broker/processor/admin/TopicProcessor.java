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
package org.apache.rocketmq.proxy.broker.processor.admin;

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.proxy.broker.BrokerController;
import org.apache.rocketmq.proxy.broker.processor.AbstractProxyProcessor;
import org.apache.rocketmq.proxy.broker.processor.DefaultRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class TopicProcessor extends AbstractProxyProcessor implements DefaultRequestProcessor {
    public TopicProcessor(BrokerController brokerController) {
        super(brokerController);
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        switch (request.getCode()) {
            case RequestCode.UPDATE_AND_CREATE_TOPIC:
            case RequestCode.DELETE_TOPIC_IN_BROKER:
                return doProxyAndReload(ctx, request);
            case RequestCode.GET_ALL_TOPIC_CONFIG:
            case RequestCode.GET_SYSTEM_TOPIC_LIST_FROM_BROKER:
                return doProxy(ctx, request, true);
            default:
                return getUnknownCmdResponse(ctx, request);
        }
    }

    private RemotingCommand doProxyAndReload(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        RemotingCommand response = doProxy(ctx, request);
        brokerController.loadAndRegisterBrokerAll();
        return response;
    }
}
