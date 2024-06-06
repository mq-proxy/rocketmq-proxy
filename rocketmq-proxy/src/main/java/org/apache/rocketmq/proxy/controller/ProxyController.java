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
package org.apache.rocketmq.proxy.controller;

import io.netty.channel.Channel;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.proxy.broker.BrokerController;
import org.apache.rocketmq.proxy.broker.processor.HeartBeatProcessor;
import org.apache.rocketmq.proxy.common.config.ProxyConfig;
import org.apache.rocketmq.proxy.common.log.LoggerName;
import org.apache.rocketmq.proxy.proxy.HeartBeatManager;
import org.apache.rocketmq.proxy.proxy.mapping.proxy.ClusterMapping;
import org.apache.rocketmq.proxy.proxy.out.ProxyOuterAPI;
import org.apache.rocketmq.proxy.proxy.processor.ProxyProcessor;
import org.apache.rocketmq.proxy.proxy.routeinfo.ProxyClusterInfo;
import org.apache.rocketmq.proxy.proxy.routeinfo.TopicRouteInfo;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingServer;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ProxyController implements Controller {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    private final ClusterMapping clusterMapping;
    private final RemotingServer remotingServer;

    private final ChannelEventListener housekeepingService = new HousekeepingService();

    private final List<Controller> controllers;

    private final NettyRequestProcessor requestProcessor;
    private final ExecutorService requestExecutor;

    private final ScheduledExecutorService scheduledExecutorService;

    // config
    private final ProxyConfig proxyConfig;
    private final BrokerConfig brokerConfig;
    private final NettyServerConfig nettyServerConfig;
    private final NettyClientConfig nettyClientConfig;

    // controller
    private final BrokerController brokerController;

    // api
    private final ProxyOuterAPI proxyOuterAPI;

    private final TopicRouteInfo topicRouteInfo;

    private final ProxyClusterInfo proxyClusterInfo;

    private final HeartBeatProcessor heartBeatProcessor;

    private final HeartBeatManager heartBeatManager;

    public ProxyController(final ProxyConfig proxyConfig,
                           final BrokerConfig brokerConfig,
                           final NettyServerConfig nettyServerConfig,
                           final NettyClientConfig nettyClientConfig) {
        this.proxyConfig = proxyConfig;
        this.brokerConfig = brokerConfig;
        this.nettyServerConfig = nettyServerConfig;
        this.nettyClientConfig = nettyClientConfig;

        this.proxyOuterAPI = new ProxyOuterAPI(nettyClientConfig);
        this.topicRouteInfo = new TopicRouteInfo(this);
        this.proxyClusterInfo = new ProxyClusterInfo(this);

        // mapping
        this.clusterMapping = new ClusterMapping(nettyClientConfig);

        this.remotingServer = new NettyRemotingServer(this.nettyServerConfig, this.housekeepingService);


        brokerController = new BrokerController(this);

        this.requestProcessor = new ProxyProcessor(this);

        heartBeatProcessor = new HeartBeatProcessor(brokerController);

        heartBeatManager = new HeartBeatManager();

        this.requestExecutor = Executors.newFixedThreadPool(this.brokerConfig.getAdminBrokerThreadPoolNums(),
                new ThreadFactoryImpl("RequestExecutorThread_"));

        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryImpl("ScheduledExecutorThread")
        );

        this.controllers = Collections.singletonList(brokerController);
    }

    @Override
    public NettyRequestProcessor getRequestProcessor() {
        return requestProcessor;
    }

    @Override
    public ChannelEventListener getChannelListener() {
        return housekeepingService;
    }

    @Override
    public void registerConfig(Properties properties) {
        for (Controller controller : controllers) {
            if (controller != null) {
                controller.registerConfig(properties);
            }
        }
    }

    @Override
    public boolean initialize() {
        this.remotingServer.registerDefaultProcessor(getRequestProcessor(), this.requestExecutor);

        if (controllers != null) {
            for (Controller controller : controllers) {
                if (controller != null) {
                    if (!controller.initialize()) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    public void startScheduled() {
        // namesrv update  cluster info(broker & proxy)
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                proxyClusterInfo.update();
            } catch (Throwable e) {
                log.info("updateNameAddress fail", e);
            }
        }, 1, 5, TimeUnit.SECONDS);

        // namesrv update route-info(broker)
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                topicRouteInfo.update();
            } catch (Throwable e) {
                log.info("updateNameAddress fail", e);
            }
        }, 2, 5, TimeUnit.SECONDS);

        // update Address Mapping
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                clusterMapping.updateProxyNameAddress(proxyClusterInfo.getProxyClusterInfo());
                clusterMapping.updateBrokerNameAddress(proxyClusterInfo.getBrokerClusterInfo());
            } catch (Throwable e) {
                log.info("updateNameAddress fail", e);
            }
        }, 2, 5, TimeUnit.SECONDS);
    }

    @Override
    public void start() {
        if (remotingServer != null) {
            remotingServer.start();
        }

        if (proxyOuterAPI != null) {
            proxyOuterAPI.updateNameServerAddressList(brokerConfig.getNamesrvAddr());
            proxyOuterAPI.start();
        }

        if (controllers != null) {
            for (Controller controller : controllers) {
                if (controller != null) {
                    controller.start();
                }
            }
        }
        startScheduled();
    }

    @Override
    public void shutdown() {
        if (remotingServer != null) {
            remotingServer.shutdown();
        }

        if (proxyOuterAPI != null) {
            proxyOuterAPI.shutdown();
        }

        if (controllers != null) {
            for (Controller controller : controllers) {
                if (controller != null) {
                    controller.shutdown();
                }
            }
        }

        scheduledExecutorService.shutdown();
    }

    public BrokerController getBrokerController() {
        return this.brokerController;
    }

    public ProxyOuterAPI getProxyOuterAPI() {
        return this.proxyOuterAPI;
    }

    public TopicRouteInfo getTopicRouteInfo() {
        return topicRouteInfo;
    }

    public ProxyClusterInfo getProxyClusterInfo() {
        return proxyClusterInfo;
    }

    public ClusterMapping getClusterMapping() {
        return clusterMapping;
    }


    public ProxyConfig getProxyConfig() {
        return proxyConfig;
    }

    public BrokerConfig getBrokerConfig() {
        return brokerConfig;
    }

    public NettyServerConfig getNettyServerConfig() {
        return nettyServerConfig;
    }

    public NettyClientConfig getNettyClientConfig() {
        return nettyClientConfig;
    }

    public RemotingServer getRemotingServer() {
        return remotingServer;
    }

    public HeartBeatProcessor getHeartBeatProcessor() {
        return heartBeatProcessor;
    }

    public HeartBeatManager getHeartBeatManager() {
        return heartBeatManager;
    }

    private class HousekeepingService implements ChannelEventListener {
        @Override
        public void onChannelConnect(String s, Channel channel) {

            for (Controller controller : controllers) {
                if (controller != null && controller.getChannelListener() != null) {
                    controller.getChannelListener().onChannelConnect(s, channel);
                }
            }
        }

        @Override
        public void onChannelClose(String s, Channel channel) {
            for (Controller controller : controllers) {
                if (controller != null && controller.getChannelListener() != null) {
                    controller.getChannelListener().onChannelClose(s, channel);
                }
            }
        }

        @Override
        public void onChannelException(String s, Channel channel) {
            for (Controller controller : controllers) {
                if (controller != null && controller.getChannelListener() != null) {
                    controller.getChannelListener().onChannelException(s, channel);
                }
            }
        }

        @Override
        public void onChannelIdle(String s, Channel channel) {
            for (Controller controller : controllers) {
                if (controller != null && controller.getChannelListener() != null) {
                    controller.getChannelListener().onChannelIdle(s, channel);
                }
            }
        }
    }
}
