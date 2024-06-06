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
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.header.GetConsumerListByGroupRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumerListByGroupResponseBody;
import org.apache.rocketmq.proxy.broker.BrokerController;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.ArrayList;
import java.util.List;

/**
 * author: G
 * create: 2024-05-10
 */
public class HeartBeatProcessor extends AbstractProxyProcessor {

    public HeartBeatProcessor(BrokerController brokerController) {
        super(brokerController);
    }

    /**
     * query clientIds by group
     *
     * @param consumerGroup
     * @return
     */
    public List<String> queryClientIdByGroup(String consumerGroup) {

        GetConsumerListByGroupRequestHeader requestHeader = new GetConsumerListByGroupRequestHeader();
        requestHeader.setConsumerGroup(consumerGroup);
        RemotingCommand req = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_LIST_BY_GROUP, requestHeader);
        req.setOpaque(RemotingCommand.createNewRequestId());

        RemotingCommand res = doProxy(null, req);
        GetConsumerListByGroupResponseBody body =
                GetConsumerListByGroupResponseBody.decode(res.getBody(), GetConsumerListByGroupResponseBody.class);
        return null == body.getConsumerIdList() ? new ArrayList<String>(0) : body.getConsumerIdList();

    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        return null;
    }

    @Override
    public boolean rejectRequest() {
        return super.rejectRequest();
    }
}
