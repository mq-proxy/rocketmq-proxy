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
package org.apache.rocketmq.proxy.proxy.processor;

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.proxy.common.log.LoggerName;
import org.apache.rocketmq.proxy.common.protocol.NotifyRegisterHeader;
import org.apache.rocketmq.proxy.common.protocol.NotifyRouteInfoGetHeader;
import org.apache.rocketmq.proxy.common.protocol.NotifySwitchHeader;
import org.apache.rocketmq.proxy.common.protocol.ProxyRequestCode;
import org.apache.rocketmq.proxy.controller.ProxyController;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class ProxyProcessor implements NettyRequestProcessor {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    public final ProxyController proxyController;

    public volatile static Long switchToBrokerTime = 0L;

    public volatile static Long switchToProxyTime = System.currentTimeMillis();
    public volatile static boolean isProxy = false;

    public final static Long HEARTBEAT_INTERVAL_AFTER_SWITCH = 40000L;

    public ProxyProcessor(ProxyController proxyController) {
        this.proxyController = proxyController;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {

        switch (request.getCode()) {
            case ProxyRequestCode.NOTIFY_PROXY_REGISTER_BROKER_OR_PROXY:
                return this.notifyProxyRegisterBroker(ctx, request);
            case ProxyRequestCode.NOTIFY_PROXY_BROKER_SWITCH:
                return this.notifyProxyBrokerSwitch(ctx, request);
            case ProxyRequestCode.NOTIFY_PROXY_ROUTE_INFO_GET:
                return this.notifyProxyRouteInfoGet(ctx, request);
            default:
                return this.proxyController.getBrokerController().getRequestProcessor().processRequest(ctx, request);
        }
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    public RemotingCommand notifyProxyRegisterBroker(ChannelHandlerContext ignore, RemotingCommand request) throws RemotingCommandException {
        NotifyRegisterHeader notifyRegisterHeader =
                (NotifyRegisterHeader) request.decodeCommandCustomHeader(NotifyRegisterHeader.class);

        // update cluster info
        this.proxyController.getProxyClusterInfo().update();

        // update memory info
        if (notifyRegisterHeader.isProxy()) {
            this.proxyController.getClusterMapping().updateProxyNameAddress(
                    this.proxyController.getProxyClusterInfo().getProxyClusterInfo()
            );
        } else {
            this.proxyController.getClusterMapping().updateBrokerNameAddress(
                    this.proxyController.getProxyClusterInfo().getBrokerClusterInfo()
            );
        }

        // update topic route
        this.proxyController.getTopicRouteInfo().update();

        log.info("notified proxy register broker: register: {} proxy: {}",
                notifyRegisterHeader.isRegister(),
                notifyRegisterHeader.isProxy());

        // invoke oneway
        return RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
    }

    public RemotingCommand notifyProxyBrokerSwitch(ChannelHandlerContext ignore, RemotingCommand request) throws RemotingCommandException {
        NotifySwitchHeader notifySwitchHeader =
                (NotifySwitchHeader) request.decodeCommandCustomHeader(NotifySwitchHeader.class);
        RemotingCommand res = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        //For the proxy, the switch has a direction and involves four state transitions. Moreover, the notifications from
        //nameserver may arrive at different order
        //Needs operation: broker->proxy, proxy->broker
        //Do nothing:    proxy->proxy, broker->broker
        if (notifySwitchHeader.isProxy()) {
            if (isProxy) {
                //proxy->proxy, do nothing
                return res;
            }
            //broker->proxy
            switchToProxyTime = System.currentTimeMillis();
            log.info("switch from broker to proxy, triggered at {}!", switchToProxyTime);
            isProxy = true;
        } else {
            if (!isProxy) {
                //broker->broker, do nothing
                return res;
            }
            // 1.proxy->broker
            switchToBrokerTime = System.currentTimeMillis();
            log.info("switch from proxy to broker, triggered at {}!", switchToBrokerTime);
            isProxy = false;
            // 2.notify all consumers
            proxyController.getBrokerController().getConsumerManager().notifyAllConsumers();
            // 3.clean record
            proxyController.getHeartBeatManager().resetRecord();
        }

        // invoke oneway
        return res;
    }

    public RemotingCommand notifyProxyRouteInfoGet(ChannelHandlerContext ignore, RemotingCommand request) throws RemotingCommandException {
        NotifyRouteInfoGetHeader notifyRouteInfoGetHeader =
                (NotifyRouteInfoGetHeader) request.decodeCommandCustomHeader(NotifyRouteInfoGetHeader.class);
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        if (switchToBrokerTime + 120000L < System.currentTimeMillis()) {
            return response;
        }
        // proxy switch back ---> notify rebalance
        if (!notifyRouteInfoGetHeader.isProxy()) {
            // notify all consumers
            proxyController.getBrokerController().getConsumerManager().notifyAllConsumers();
        }

        return response;
    }
}
