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

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.ReplyMessageRequestHeader;
import org.apache.rocketmq.proxy.broker.BrokerController;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.Map;

public abstract class AbstractReplyProcessor extends AbstractProxyProcessor {

    public AbstractReplyProcessor(BrokerController brokerController) {
        super(brokerController);
    }

    // redirect to producer
    protected RemotingCommand replyMessage(ChannelHandlerContext ignore, RemotingCommand request) throws RemotingCommandException {
        ReplyMessageRequestHeader requestHeader =
                (ReplyMessageRequestHeader) request.decodeCommandCustomHeader(ReplyMessageRequestHeader.class);
        Map<String, String> props = MessageDecoder.string2messageProperties(requestHeader.getProperties());
        if (props != null) {
            String senderId = props.get(MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT);
            Channel channel = brokerController.getProducerManager().findChannel(senderId);
            try {
                return this.brokerController.getBroker2Client().callClient(channel, request);
            } catch (RemotingSendRequestException | RemotingTimeoutException | InterruptedException e) {
                throw new RemotingCommandException(e.getMessage());
            }
        }
        return RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, "reply sender not found");
    }
}
