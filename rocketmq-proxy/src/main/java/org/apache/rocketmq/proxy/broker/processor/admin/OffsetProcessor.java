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
import org.apache.rocketmq.proxy.broker.processor.DefaultRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class OffsetProcessor extends AbstractAdminProcessor implements DefaultRequestProcessor {
    public OffsetProcessor(BrokerController brokerController) {
        super(brokerController);
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        switch (request.getCode()) {
            case RequestCode.SEARCH_OFFSET_BY_TIMESTAMP:
            case RequestCode.GET_MAX_OFFSET:
            case RequestCode.GET_MIN_OFFSET:
            case RequestCode.GET_ALL_CONSUMER_OFFSET:
            case RequestCode.GET_ALL_DELAY_OFFSET:
            case RequestCode.QUERY_CORRECTION_OFFSET:
                return doProxy(ctx, request, true);
            case RequestCode.INVOKE_BROKER_TO_RESET_OFFSET:
            case RequestCode.CLONE_GROUP_OFFSET:
                return doProxy(ctx, request);
            default:
                return getUnknownCmdResponse(ctx, request);
        }
    }
}
