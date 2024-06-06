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
package org.apache.rocketmq.proxy.broker;

import com.google.common.collect.Lists;
import org.apache.rocketmq.acl.AccessValidator;
import org.apache.rocketmq.proxy.broker.client.ClientHousekeepingService;
import org.apache.rocketmq.proxy.broker.client.ConsumerIdsChangeListener;
import org.apache.rocketmq.proxy.broker.client.ConsumerManager;
import org.apache.rocketmq.proxy.broker.client.DefaultConsumerIdsChangeListener;
import org.apache.rocketmq.proxy.broker.client.ProducerManager;
import org.apache.rocketmq.proxy.broker.client.net.Broker2Client;
import org.apache.rocketmq.proxy.broker.latency.BrokerFastFailure;
import org.apache.rocketmq.proxy.broker.latency.BrokerFixedThreadPoolExecutor;
import org.apache.rocketmq.proxy.broker.mqtrace.ConsumeMessageHook;
import org.apache.rocketmq.proxy.broker.mqtrace.SendMessageHook;
import org.apache.rocketmq.proxy.broker.netty.ProxyRemotingClient;
import org.apache.rocketmq.proxy.broker.out.BrokerOuterAPI;
import org.apache.rocketmq.proxy.broker.processor.AdminBrokerProcessor;
import org.apache.rocketmq.proxy.broker.processor.ClientManageProcessor;
import org.apache.rocketmq.proxy.broker.processor.ConsumerManageProcessor;
import org.apache.rocketmq.proxy.broker.processor.EndTransactionProcessor;
import org.apache.rocketmq.proxy.broker.processor.PullMessageProcessor;
import org.apache.rocketmq.proxy.broker.processor.QueryMessageProcessor;
import org.apache.rocketmq.proxy.broker.processor.ReplyMessageProcessor;
import org.apache.rocketmq.proxy.broker.processor.SendMessageProcessor;
import org.apache.rocketmq.proxy.broker.processor.client.ClientProcessor;
import org.apache.rocketmq.proxy.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.proxy.broker.topic.TopicConfigManager;
import org.apache.rocketmq.proxy.broker.util.ServiceProvider;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.Configuration;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.namesrv.RegisterBrokerResult;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.common.stats.MomentStatsItem;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.proxy.common.log.LoggerName;
import org.apache.rocketmq.proxy.controller.Controller;
import org.apache.rocketmq.proxy.controller.ProxyController;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.common.TlsMode;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.netty.NettyRemotingServer;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.netty.RequestTask;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.srvutil.FileWatchService;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class BrokerController implements Controller {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    private static final InternalLogger LOG_PROTECTION = InternalLoggerFactory.getLogger(org.apache.rocketmq.common.constant.LoggerName.PROTECTION_LOGGER_NAME);
    private static final InternalLogger LOG_WATER_MARK = InternalLoggerFactory.getLogger(org.apache.rocketmq.common.constant.LoggerName.WATER_MARK_LOGGER_NAME);
    private final ProxyController proxyController;
    private final ConsumerManager consumerManager;
    private final ProducerManager producerManager;
    private final ClientHousekeepingService clientHousekeepingService;
    private final PullMessageProcessor pullMessageProcessor;
    private final Broker2Client broker2Client;
    private final SubscriptionGroupManager subscriptionGroupManager;
    private final ConsumerIdsChangeListener consumerIdsChangeListener;
    private final BrokerOuterAPI brokerOuterAPI;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl(
            "BrokerControllerScheduledThread"));
    private final BlockingQueue<Runnable> sendThreadPoolQueue;
    private final BlockingQueue<Runnable> putThreadPoolQueue;
    private final BlockingQueue<Runnable> pullThreadPoolQueue;
    private final BlockingQueue<Runnable> replyThreadPoolQueue;
    private final BlockingQueue<Runnable> queryThreadPoolQueue;
    private final BlockingQueue<Runnable> clientManagerThreadPoolQueue;
    private final BlockingQueue<Runnable> consumerManagerThreadPoolQueue;
    private final BlockingQueue<Runnable> heartbeatThreadPoolQueue;
    private final BlockingQueue<Runnable> endTransactionThreadPoolQueue;
    private final BrokerStatsManager brokerStatsManager;
    private final List<SendMessageHook> sendMessageHookList = new ArrayList<>();
    private final List<ConsumeMessageHook> consumeMessageHookList = new ArrayList<>();
    private final BrokerFastFailure brokerFastFailure;
    private final Configuration configuration;
    private final Map<Class<?>, AccessValidator> accessValidatorMap = new HashMap<>();
    private RemotingServer remotingServer;

    private final RemotingClient remotingClient;

    private final AdminBrokerProcessor adminBrokerProcessor = new AdminBrokerProcessor(this);
    private final ClientProcessor clientProcessor = new ClientProcessor(this);

    private TopicConfigManager topicConfigManager;
    private ExecutorService sendMessageExecutor;
    private ExecutorService putMessageFutureExecutor;
    private ExecutorService pullMessageExecutor;
    private ExecutorService replyMessageExecutor;
    private ExecutorService queryMessageExecutor;
    private ExecutorService adminBrokerExecutor;
    private ExecutorService clientManageExecutor;
    private ExecutorService heartbeatExecutor;
    private ExecutorService consumerManageExecutor;
    private ExecutorService endTransactionExecutor;

    /*
     * client executor
     */
    private ExecutorService clientExecutor;

    private final MockBrokerStats brokerStats;
    private FileWatchService fileWatchService;

    private final ClientManageProcessor clientManageProcessor;

    public BrokerController(final ProxyController proxyController) {
        proxyController.getNettyClientConfig().setConnectTimeoutMillis(30000);
        this.remotingServer = proxyController.getRemotingServer();
        this.remotingClient = new NettyRemotingClient(proxyController.getNettyClientConfig());
        this.proxyController = proxyController;
        this.topicConfigManager = new TopicConfigManager(this);
        this.pullMessageProcessor = new PullMessageProcessor(this);
        this.consumerIdsChangeListener = new DefaultConsumerIdsChangeListener(this);
        this.consumerManager = new ConsumerManager(this.consumerIdsChangeListener);
        this.producerManager = new ProducerManager();
        this.clientHousekeepingService = new ClientHousekeepingService(this);
        this.broker2Client = new Broker2Client(this);
        this.subscriptionGroupManager = new SubscriptionGroupManager(this);
        this.brokerOuterAPI = new BrokerOuterAPI(proxyController.getNettyClientConfig());
        this.clientManageProcessor = new ClientManageProcessor(this, proxyController);

        BrokerConfig brokerConfig = proxyController.getBrokerConfig();
        this.sendThreadPoolQueue = new LinkedBlockingQueue<>(brokerConfig.getSendThreadPoolQueueCapacity());
        this.putThreadPoolQueue = new LinkedBlockingQueue<>(brokerConfig.getPutThreadPoolQueueCapacity());
        this.pullThreadPoolQueue = new LinkedBlockingQueue<>(brokerConfig.getPullThreadPoolQueueCapacity());
        this.replyThreadPoolQueue = new LinkedBlockingQueue<>(brokerConfig.getReplyThreadPoolQueueCapacity());
        this.queryThreadPoolQueue = new LinkedBlockingQueue<>(brokerConfig.getQueryThreadPoolQueueCapacity());
        this.clientManagerThreadPoolQueue = new LinkedBlockingQueue<>(brokerConfig.getClientManagerThreadPoolQueueCapacity());
        this.consumerManagerThreadPoolQueue = new LinkedBlockingQueue<>(brokerConfig.getConsumerManagerThreadPoolQueueCapacity());
        this.heartbeatThreadPoolQueue = new LinkedBlockingQueue<>(brokerConfig.getHeartbeatThreadPoolQueueCapacity());
        this.endTransactionThreadPoolQueue = new LinkedBlockingQueue<>(brokerConfig.getEndTransactionPoolQueueCapacity());

        this.brokerStatsManager = new BrokerStatsManager(brokerConfig.getBrokerClusterName(), brokerConfig.isEnableDetailStat());

        this.brokerFastFailure = new BrokerFastFailure(this);
        this.configuration = new Configuration(
                log,
                BrokerPathConfigHelper.getBrokerConfigPath(),
                brokerConfig,
                proxyController.getNettyServerConfig(),
                proxyController.getNettyClientConfig()
        );

        this.brokerStats = new MockBrokerStats();
    }

    @Override
    public void registerConfig(Properties properties) {
        this.configuration.registerConfig(properties);
    }

    public ProxyController getProxyController() {
        return proxyController;
    }

    public BrokerConfig getBrokerConfig() {
        return proxyController.getBrokerConfig();
    }

    public NettyServerConfig getNettyServerConfig() {
        return proxyController.getNettyServerConfig();
    }

    public NettyClientConfig getNettyClientConfig() {
        return proxyController.getNettyClientConfig();
    }

    public BlockingQueue<Runnable> getPullThreadPoolQueue() {
        return pullThreadPoolQueue;
    }

    public BlockingQueue<Runnable> getQueryThreadPoolQueue() {
        return queryThreadPoolQueue;
    }

    @Override
    public NettyRequestProcessor getRequestProcessor() {
        return adminBrokerProcessor;
    }

    public ClientManageProcessor getClientManageProcessor() {
        return clientManageProcessor;
    }

    @Override
    public ChannelEventListener getChannelListener() {
        return clientHousekeepingService;
    }

    public boolean initialize() {
        boolean result = this.topicConfigManager.load();
        result = result && this.subscriptionGroupManager.load();

        if (!result) {
            return false;
        }

        this.remotingClient.start();

        this.sendMessageExecutor = new BrokerFixedThreadPoolExecutor(
                this.getBrokerConfig().getSendMessageThreadPoolNums(),
                this.getBrokerConfig().getSendMessageThreadPoolNums(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.sendThreadPoolQueue,
                new ThreadFactoryImpl("SendMessageThread_"));

        this.putMessageFutureExecutor = new BrokerFixedThreadPoolExecutor(
                this.getBrokerConfig().getPutMessageFutureThreadPoolNums(),
                this.getBrokerConfig().getPutMessageFutureThreadPoolNums(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.putThreadPoolQueue,
                new ThreadFactoryImpl("PutMessageThread_"));

        this.pullMessageExecutor = new BrokerFixedThreadPoolExecutor(
                this.getBrokerConfig().getPullMessageThreadPoolNums(),
                this.getBrokerConfig().getPullMessageThreadPoolNums(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.pullThreadPoolQueue,
                new ThreadFactoryImpl("PullMessageThread_"));

        this.replyMessageExecutor = new BrokerFixedThreadPoolExecutor(
                this.getBrokerConfig().getProcessReplyMessageThreadPoolNums(),
                this.getBrokerConfig().getProcessReplyMessageThreadPoolNums(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.replyThreadPoolQueue,
                new ThreadFactoryImpl("ProcessReplyMessageThread_"));

        this.queryMessageExecutor = new BrokerFixedThreadPoolExecutor(
                this.getBrokerConfig().getQueryMessageThreadPoolNums(),
                this.getBrokerConfig().getQueryMessageThreadPoolNums(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.queryThreadPoolQueue,
                new ThreadFactoryImpl("QueryMessageThread_"));

        this.adminBrokerExecutor =
                Executors.newFixedThreadPool(this.getBrokerConfig().getAdminBrokerThreadPoolNums(), new ThreadFactoryImpl(
                        "AdminBrokerThread_"));

        this.clientManageExecutor = new ThreadPoolExecutor(
                this.getBrokerConfig().getClientManageThreadPoolNums(),
                this.getBrokerConfig().getClientManageThreadPoolNums(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.clientManagerThreadPoolQueue,
                new ThreadFactoryImpl("ClientManageThread_"));

        this.heartbeatExecutor = new BrokerFixedThreadPoolExecutor(
                this.getBrokerConfig().getHeartbeatThreadPoolNums(),
                this.getBrokerConfig().getHeartbeatThreadPoolNums(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.heartbeatThreadPoolQueue,
                new ThreadFactoryImpl("HeartbeatThread_", true));

        this.endTransactionExecutor = new BrokerFixedThreadPoolExecutor(
                this.getBrokerConfig().getEndTransactionThreadPoolNums(),
                this.getBrokerConfig().getEndTransactionThreadPoolNums(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.endTransactionThreadPoolQueue,
                new ThreadFactoryImpl("EndTransactionThread_"));

        this.consumerManageExecutor = Executors.newFixedThreadPool(
                this.getBrokerConfig().getConsumerManageThreadPoolNums(),
                new ThreadFactoryImpl("ConsumerManageThread_"));

        this.clientExecutor =
                Executors.newFixedThreadPool(this.getBrokerConfig().getConsumerManageThreadPoolNums(), new ThreadFactoryImpl(
                        "ClientThread_"));

        this.registerProcessor();

        this.registerClientProcessor();

        final long initialDelay = UtilAll.computeNextMorningTimeMillis() - System.currentTimeMillis();
        final long period = 1000 * 60 * 60 * 24;
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                BrokerController.this.getBrokerStats().recordOneDay();
            } catch (Throwable e) {
                log.error("schedule record error.", e);
            }
        }, initialDelay, period, TimeUnit.MILLISECONDS);
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                BrokerController.this.getBrokerStats().recordOneSecond();
            } catch (Throwable e) {
                log.error("schedule record error.", e);
            }
        }, 1, 1, TimeUnit.SECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                BrokerController.this.protectBroker();
            } catch (Throwable e) {
                log.error("protectBroker error.", e);
            }
        }, 3, 3, TimeUnit.MINUTES);

        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                BrokerController.this.printWaterMark();
            } catch (Throwable e) {
                log.error("printWaterMark error.", e);
            }
        }, 10, 1, TimeUnit.SECONDS);

        if (this.getBrokerConfig().getNamesrvAddr() != null) {
            this.brokerOuterAPI.updateNameServerAddressList(this.getBrokerConfig().getNamesrvAddr());
            log.info("Set user specified name server address: {}", this.getBrokerConfig().getNamesrvAddr());
            this.scheduledExecutorService.scheduleAtFixedRate(() -> {
                try {
                    BrokerController.this.brokerOuterAPI.updateNameServerAddressList(BrokerController.this.getBrokerConfig().getNamesrvAddr());
                } catch (Throwable e) {
                    log.error("ScheduledTask updateNameServerAddr exception", e);
                }
            }, 1000 * 10, 1000 * 60 * 2, TimeUnit.MILLISECONDS);
        } else if (this.getBrokerConfig().isFetchNamesrvAddrByAddressServer()) {
            this.scheduledExecutorService.scheduleAtFixedRate(() -> {
                try {
                    BrokerController.this.brokerOuterAPI.fetchNameServerAddr();
                } catch (Throwable e) {
                    log.error("ScheduledTask fetchNameServerAddr exception", e);
                }
            }, 1000 * 10, 1000 * 60 * 2, TimeUnit.MILLISECONDS);
        }

        if (TlsSystemConfig.tlsMode != TlsMode.DISABLED) {
            // Register a listener to reload SslContext
            try {
                fileWatchService = new FileWatchService(
                        new String[]{
                            TlsSystemConfig.tlsServerCertPath,
                            TlsSystemConfig.tlsServerKeyPath,
                            TlsSystemConfig.tlsServerTrustCertPath
                        },
                        new FileWatchService.Listener() {
                            boolean certChanged, keyChanged = false;

                            @Override
                            public void onChanged(String path) {
                                if (path.equals(TlsSystemConfig.tlsServerTrustCertPath)) {
                                    log.info("The trust certificate changed, reload the ssl context");
                                    reloadServerSslContext();
                                }
                                if (path.equals(TlsSystemConfig.tlsServerCertPath)) {
                                    certChanged = true;
                                }
                                if (path.equals(TlsSystemConfig.tlsServerKeyPath)) {
                                    keyChanged = true;
                                }
                                if (certChanged && keyChanged) {
                                    log.info("The certificate and private key changed, reload the ssl context");
                                    certChanged = keyChanged = false;
                                    reloadServerSslContext();
                                }
                            }

                            private void reloadServerSslContext() {
                                ((NettyRemotingServer) remotingServer).loadSslContext();
                            }
                        });
            } catch (Exception e) {
                log.warn("FileWatchService created error, can't load the certificate dynamically");
            }
        }
        initialAcl();
        initialRpcHooks();
        return true;
    }

    private void initialAcl() {
        if (!this.getBrokerConfig().isAclEnable()) {
            log.info("The broker dose not enable acl");
            return;
        }

        List<AccessValidator> accessValidators = ServiceProvider.load(ServiceProvider.ACL_VALIDATOR_ID, AccessValidator.class);
        if (accessValidators.isEmpty()) {
            log.info("The broker dose not load the AccessValidator");
            return;
        }

        for (AccessValidator accessValidator : accessValidators) {
            final AccessValidator validator = accessValidator;
            accessValidatorMap.put(validator.getClass(), validator);
            this.registerServerRPCHook(new RPCHook() {

                @Override
                public void doBeforeRequest(String remoteAddr, RemotingCommand request) {
                    //Do not catch the exception
                    validator.validate(validator.parse(request, remoteAddr));
                }

                @Override
                public void doAfterResponse(String remoteAddr, RemotingCommand request, RemotingCommand response) {
                }
            });
        }
    }

    private void initialRpcHooks() {
        List<RPCHook> rpcHooks = ServiceProvider.load(ServiceProvider.RPC_HOOK_ID, RPCHook.class);
        if (rpcHooks.isEmpty()) {
            return;
        }
        for (RPCHook rpcHook : rpcHooks) {
            this.registerServerRPCHook(rpcHook);
        }
    }

    public void registerProcessor() {
        /*
         * SendMessageProcessor
         */
        SendMessageProcessor sendProcessor = new SendMessageProcessor(this);
        sendProcessor.registerSendMessageHook(sendMessageHookList);

        this.remotingServer.registerProcessor(RequestCode.SEND_MESSAGE, sendProcessor, this.sendMessageExecutor);
        this.remotingServer.registerProcessor(RequestCode.SEND_MESSAGE_V2, sendProcessor, this.sendMessageExecutor);
        this.remotingServer.registerProcessor(RequestCode.SEND_BATCH_MESSAGE, sendProcessor, this.sendMessageExecutor);
        this.remotingServer.registerProcessor(RequestCode.CONSUMER_SEND_MSG_BACK, sendProcessor, this.sendMessageExecutor);
        /*
         * PullMessageProcessor
         */
        this.remotingServer.registerProcessor(RequestCode.PULL_MESSAGE, this.pullMessageProcessor, this.pullMessageExecutor);
        this.pullMessageProcessor.registerConsumeMessageHook(consumeMessageHookList);

        /*
         * ReplyMessageProcessor
         */
        ReplyMessageProcessor replyMessageProcessor = new ReplyMessageProcessor(this);
        replyMessageProcessor.registerSendMessageHook(sendMessageHookList);

        this.remotingServer.registerProcessor(RequestCode.SEND_REPLY_MESSAGE, replyMessageProcessor, replyMessageExecutor);
        this.remotingServer.registerProcessor(RequestCode.SEND_REPLY_MESSAGE_V2, replyMessageProcessor, replyMessageExecutor);

        /*
          QueryMessageProcessor
         */
        NettyRequestProcessor queryProcessor = new QueryMessageProcessor(this);
        this.remotingServer.registerProcessor(RequestCode.QUERY_MESSAGE, queryProcessor, this.queryMessageExecutor);
        this.remotingServer.registerProcessor(RequestCode.VIEW_MESSAGE_BY_ID, queryProcessor, this.queryMessageExecutor);

        /*
         * ClientManageProcessor
         */
        this.remotingServer.registerProcessor(RequestCode.HEART_BEAT, this.clientManageProcessor, this.heartbeatExecutor);
        this.remotingServer.registerProcessor(RequestCode.UNREGISTER_CLIENT, this.clientManageProcessor, this.clientManageExecutor);
        this.remotingServer.registerProcessor(RequestCode.CHECK_CLIENT_CONFIG, this.clientManageProcessor, this.clientManageExecutor);

        /*
         * ConsumerManageProcessor
         */
        ConsumerManageProcessor consumerManageProcessor = new ConsumerManageProcessor(this);
        this.remotingServer.registerProcessor(RequestCode.GET_CONSUMER_LIST_BY_GROUP, consumerManageProcessor, this.consumerManageExecutor);
        this.remotingServer.registerProcessor(RequestCode.UPDATE_CONSUMER_OFFSET, consumerManageProcessor, this.consumerManageExecutor);
        this.remotingServer.registerProcessor(RequestCode.QUERY_CONSUMER_OFFSET, consumerManageProcessor, this.consumerManageExecutor);

        /*
         * EndTransactionProcessor
         */
        this.remotingServer.registerProcessor(RequestCode.END_TRANSACTION, new EndTransactionProcessor(this), this.endTransactionExecutor);
    }

    public void registerClientProcessor() {
        registerClientProcessor(this.remotingClient);
    }

    public void registerClientProcessor(RemotingClient client) {
        if (client != null) {
            clientProcessor.register(client, this.clientExecutor);
        }
    }

    public void registerClientProcessor(ProxyRemotingClient client) {
        if (client != null) {
            clientProcessor.register(client, this.clientExecutor);
        }
    }

    public MockBrokerStats getBrokerStats() {
        return brokerStats;
    }

    public void protectBroker() {
        if (this.getBrokerConfig().isDisableConsumeIfConsumerReadSlowly()) {
            for (Map.Entry<String, MomentStatsItem> next : this.brokerStatsManager.getMomentStatsItemSetFallSize().getStatsItemTable().entrySet()) {
                final long fallBehindBytes = next.getValue().getValue().get();
                if (fallBehindBytes > this.getBrokerConfig().getConsumerFallbehindThreshold()) {
                    final String[] split = next.getValue().getStatsKey().split("@");
                    final String group = split[2];
                    LOG_PROTECTION.info("[PROTECT_BROKER] the consumer[{}] consume slowly, {} bytes, disable it", group, fallBehindBytes);
                    this.subscriptionGroupManager.disableConsume(group);
                }
            }
        }
    }

    public long headSlowTimeMills(BlockingQueue<Runnable> q) {
        long slowTimeMills = 0;
        final Runnable peek = q.peek();
        if (peek != null) {
            RequestTask rt = BrokerFastFailure.castRunnable(peek);
            slowTimeMills = rt == null ? 0 : System.currentTimeMillis() - rt.getCreateTimestamp();
        }

        if (slowTimeMills < 0) {
            slowTimeMills = 0;
        }

        return slowTimeMills;
    }

    public long headSlowTimeMills4SendThreadPoolQueue() {
        return this.headSlowTimeMills(this.sendThreadPoolQueue);
    }

    public long headSlowTimeMills4PullThreadPoolQueue() {
        return this.headSlowTimeMills(this.pullThreadPoolQueue);
    }

    public long headSlowTimeMills4QueryThreadPoolQueue() {
        return this.headSlowTimeMills(this.queryThreadPoolQueue);
    }

    public long headSlowTimeMills4EndTransactionThreadPoolQueue() {
        return this.headSlowTimeMills(this.endTransactionThreadPoolQueue);
    }

    public void printWaterMark() {
        LOG_WATER_MARK.info("[WATERMARK] Send Queue Size: {} SlowTimeMills: {}", this.sendThreadPoolQueue.size(), headSlowTimeMills4SendThreadPoolQueue());
        LOG_WATER_MARK.info("[WATERMARK] Pull Queue Size: {} SlowTimeMills: {}", this.pullThreadPoolQueue.size(), headSlowTimeMills4PullThreadPoolQueue());
        LOG_WATER_MARK.info("[WATERMARK] Query Queue Size: {} SlowTimeMills: {}", this.queryThreadPoolQueue.size(), headSlowTimeMills4QueryThreadPoolQueue());
        LOG_WATER_MARK.info("[WATERMARK] Transaction Queue Size: {} SlowTimeMills: {}", this.endTransactionThreadPoolQueue.size(), headSlowTimeMills4EndTransactionThreadPoolQueue());
    }

    public Broker2Client getBroker2Client() {
        return broker2Client;
    }

    public ConsumerManager getConsumerManager() {
        return consumerManager;
    }

    public ProducerManager getProducerManager() {
        return producerManager;
    }

    public SubscriptionGroupManager getSubscriptionGroupManager() {
        return subscriptionGroupManager;
    }

    public void shutdown() {
        if (this.brokerStatsManager != null) {
            this.brokerStatsManager.shutdown();
        }

        if (this.clientHousekeepingService != null) {
            this.clientHousekeepingService.shutdown();
        }

        remotingClient.shutdown();

        if (this.fileWatchService != null) {
            this.fileWatchService.shutdown();
        }

        this.scheduledExecutorService.shutdown();
        try {
            if (!this.scheduledExecutorService.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                log.info("await termination failed");
            }
        } catch (InterruptedException e) {
            log.warn("interrupted while waiting for scheduler shutdown", e);
        }

        this.unregisterBrokerAll();

        if (this.sendMessageExecutor != null) {
            this.sendMessageExecutor.shutdown();
        }

        if (this.putMessageFutureExecutor != null) {
            this.putMessageFutureExecutor.shutdown();
        }

        if (this.pullMessageExecutor != null) {
            this.pullMessageExecutor.shutdown();
        }

        if (this.replyMessageExecutor != null) {
            this.replyMessageExecutor.shutdown();
        }

        if (this.adminBrokerExecutor != null) {
            this.adminBrokerExecutor.shutdown();
        }

        if (this.brokerOuterAPI != null) {
            this.brokerOuterAPI.shutdown();
        }

        if (this.brokerFastFailure != null) {
            this.brokerFastFailure.shutdown();
        }

        if (this.clientManageExecutor != null) {
            this.clientManageExecutor.shutdown();
        }

        if (this.queryMessageExecutor != null) {
            this.queryMessageExecutor.shutdown();
        }

        if (this.consumerManageExecutor != null) {
            this.consumerManageExecutor.shutdown();
        }

        if (this.fileWatchService != null) {
            this.fileWatchService.shutdown();
        }

        if (this.endTransactionExecutor != null) {
            this.endTransactionExecutor.shutdown();
        }
    }

    private void unregisterBrokerAll() {
        this.brokerOuterAPI.unregisterBrokerAll(
                this.getBrokerConfig().getBrokerClusterName(),
                this.getBrokerAddr(),
                this.getBrokerConfig().getBrokerName(),
                this.getBrokerConfig().getBrokerId());
    }

    public String getBrokerAddr() {
        return this.getBrokerConfig().getBrokerIP1() + ":" + this.getNettyServerConfig().getListenPort();
    }

    @Override
    public void start() {
        if (this.fileWatchService != null) {
            this.fileWatchService.start();
        }

        if (this.brokerOuterAPI != null) {
            this.brokerOuterAPI.start();
        }

        if (this.clientHousekeepingService != null) {
            this.clientHousekeepingService.start();
        }

        this.loadAndRegisterBrokerAll();

        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                BrokerController.this.loadAndRegisterBrokerAll();
            } catch (Throwable e) {
                log.error("registerBrokerAll Exception", e);
            }
        }, 1000 * 10, Math.max(10000, Math.min(getBrokerConfig().getRegisterNameServerPeriod(), 60000)), TimeUnit.MILLISECONDS);

        if (this.brokerStatsManager != null) {
            this.brokerStatsManager.start();
        }

        if (this.brokerFastFailure != null) {
            this.brokerFastFailure.start();
        }
    }

    public synchronized void registerIncrementBrokerData(TopicConfig topicConfig, DataVersion dataVersion) {
        TopicConfig registerTopicConfig = topicConfig;
        if (!PermName.isWriteable(this.getBrokerConfig().getBrokerPermission())
                || !PermName.isReadable(this.getBrokerConfig().getBrokerPermission())) {
            registerTopicConfig =
                    new TopicConfig(topicConfig.getTopicName(), topicConfig.getReadQueueNums(), topicConfig.getWriteQueueNums(),
                            this.getBrokerConfig().getBrokerPermission());
        }

        ConcurrentMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<String, TopicConfig>();
        topicConfigTable.put(topicConfig.getTopicName(), registerTopicConfig);
        TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();
        topicConfigSerializeWrapper.setDataVersion(dataVersion);
        topicConfigSerializeWrapper.setTopicConfigTable(topicConfigTable);

        doRegisterBrokerAll(true, false, topicConfigSerializeWrapper);
    }

    public synchronized void loadAndRegisterBrokerAll() {
        this.getTopicConfigManager().load();
        registerBrokerAll(true, true, true);
    }

    public synchronized void registerBrokerAll(final boolean checkOrderConfig, boolean oneway, boolean forceRegister) {
        TopicConfigSerializeWrapper topicConfigWrapper = this.getTopicConfigManager().buildTopicConfigSerializeWrapper();

        if (!PermName.isWriteable(this.getBrokerConfig().getBrokerPermission())
                || !PermName.isReadable(this.getBrokerConfig().getBrokerPermission())) {
            ConcurrentHashMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<>();
            for (TopicConfig topicConfig : topicConfigWrapper.getTopicConfigTable().values()) {
                TopicConfig tmp =
                        new TopicConfig(topicConfig.getTopicName(), topicConfig.getReadQueueNums(), topicConfig.getWriteQueueNums(),
                                this.getBrokerConfig().getBrokerPermission());
                topicConfigTable.put(topicConfig.getTopicName(), tmp);
            }
            topicConfigWrapper.setTopicConfigTable(topicConfigTable);
        }

        if (forceRegister || needRegister(this.getBrokerConfig().getBrokerClusterName(),
                this.getBrokerAddr(),
                this.getBrokerConfig().getBrokerName(),
                this.getBrokerConfig().getBrokerId(),
                this.getBrokerConfig().getRegisterBrokerTimeoutMills())) {
            doRegisterBrokerAll(checkOrderConfig, oneway, topicConfigWrapper);
        }
    }

    private void doRegisterBrokerAll(boolean checkOrderConfig, boolean oneway,
                                     TopicConfigSerializeWrapper topicConfigWrapper) {
        List<RegisterBrokerResult> registerBrokerResultList = this.brokerOuterAPI.registerBrokerAll(
                this.getBrokerConfig().getBrokerClusterName(),
                this.getBrokerAddr(),
                this.getBrokerConfig().getBrokerName(),
                this.getBrokerConfig().getBrokerId(),
                this.getHAServerAddr(),
                topicConfigWrapper,
                Lists.newArrayList(),
                oneway,
                this.getBrokerConfig().getRegisterBrokerTimeoutMills(),
                this.getBrokerConfig().isCompressedRegister());

        if (!registerBrokerResultList.isEmpty()) {
            RegisterBrokerResult registerBrokerResult = registerBrokerResultList.get(0);
            if (registerBrokerResult != null) {
                if (checkOrderConfig) {
                    this.getTopicConfigManager().updateOrderTopicConfig(registerBrokerResult.getKvTable());
                }
            }
        }
    }

    private boolean needRegister(final String clusterName,
                                 final String brokerAddr,
                                 final String brokerName,
                                 final long brokerId,
                                 final int timeoutMills) {

        TopicConfigSerializeWrapper topicConfigWrapper = this.getTopicConfigManager().buildTopicConfigSerializeWrapper();
        List<Boolean> changeList = brokerOuterAPI.needRegister(clusterName, brokerAddr, brokerName, brokerId, topicConfigWrapper, timeoutMills);
        boolean needRegister = false;
        for (Boolean changed : changeList) {
            if (changed) {
                needRegister = true;
                break;
            }
        }
        return needRegister;
    }

    public TopicConfigManager getTopicConfigManager() {
        return topicConfigManager;
    }

    public void setTopicConfigManager(TopicConfigManager topicConfigManager) {
        this.topicConfigManager = topicConfigManager;
    }

    public String getHAServerAddr() {
        return this.getBrokerConfig().getBrokerIP2() + ":0";
    }

    public BlockingQueue<Runnable> getSendThreadPoolQueue() {
        return sendThreadPoolQueue;
    }

    public BrokerStatsManager getBrokerStatsManager() {
        return brokerStatsManager;
    }

    public List<SendMessageHook> getSendMessageHookList() {
        return sendMessageHookList;
    }

    public void registerSendMessageHook(final SendMessageHook hook) {
        this.sendMessageHookList.add(hook);
        log.info("register SendMessageHook Hook, {}", hook.hookName());
    }

    public List<ConsumeMessageHook> getConsumeMessageHookList() {
        return consumeMessageHookList;
    }

    public void registerConsumeMessageHook(final ConsumeMessageHook hook) {
        this.consumeMessageHookList.add(hook);
        log.info("register ConsumeMessageHook Hook, {}", hook.hookName());
    }

    public void registerServerRPCHook(RPCHook rpcHook) {
        getRemotingServer().registerRPCHook(rpcHook);
    }

    public RemotingServer getRemotingServer() {
        return remotingServer;
    }

    public void setRemotingServer(RemotingServer remotingServer) {
        this.remotingServer = remotingServer;
    }

    public RemotingClient getRemotingClient() {
        return remotingClient;
    }

    public void registerClientRPCHook(RPCHook rpcHook) {
        this.getBrokerOuterAPI().registerRPCHook(rpcHook);
    }

    public BrokerOuterAPI getBrokerOuterAPI() {
        return brokerOuterAPI;
    }

    public Configuration getConfiguration() {
        return this.configuration;
    }

    public BlockingQueue<Runnable> getHeartbeatThreadPoolQueue() {
        return heartbeatThreadPoolQueue;
    }

    public Map<Class<?>, AccessValidator> getAccessValidatorMap() {
        return accessValidatorMap;
    }

    public ExecutorService getPutMessageFutureExecutor() {
        return putMessageFutureExecutor;
    }
}
