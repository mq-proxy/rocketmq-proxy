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
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.GetBrokerConfigResponseHeader;
import org.apache.rocketmq.proxy.broker.BrokerController;
import org.apache.rocketmq.proxy.broker.processor.AbstractProxyProcessor;
import org.apache.rocketmq.proxy.broker.processor.DefaultRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.io.UnsupportedEncodingException;

public class ConfigProcessor extends AbstractProxyProcessor implements DefaultRequestProcessor {
    public ConfigProcessor(BrokerController brokerController) {
        super(brokerController);
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        switch (request.getCode()) {
            case RequestCode.GET_BROKER_CONFIG:
                return getBrokerConfig(ctx, request);
            case RequestCode.UPDATE_AND_CREATE_ACL_CONFIG:      // TODO unsupported
            case RequestCode.DELETE_ACL_CONFIG:                 // TODO unsupported
            case RequestCode.GET_BROKER_CLUSTER_ACL_INFO:       // TODO unsupported
            case RequestCode.UPDATE_GLOBAL_WHITE_ADDRS_CONFIG:  // TODO unsupported
            case RequestCode.GET_BROKER_CLUSTER_ACL_CONFIG:     // TODO unsupported
            case RequestCode.UPDATE_BROKER_CONFIG:              // TODO unsupported
            default:
                return getUnknownCmdResponse(ctx, request);
        }
    }

    private RemotingCommand getBrokerConfig(ChannelHandlerContext ignore, RemotingCommand ignore0) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(GetBrokerConfigResponseHeader.class);
        final GetBrokerConfigResponseHeader responseHeader = (GetBrokerConfigResponseHeader) response.readCustomHeader();

        String content = this.brokerController.getConfiguration().getAllConfigsFormatString();
        if (content != null && !content.isEmpty()) {
            try {
                response.setBody(content.getBytes(MixAll.DEFAULT_CHARSET));
            } catch (UnsupportedEncodingException e) {
                log.error("", e);

                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }
        }

        responseHeader.setVersion(this.brokerController.getConfiguration().getDataVersionJson());

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }
}
