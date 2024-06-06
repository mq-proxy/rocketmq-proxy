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
package org.apache.rocketmq.namesrv.processor;

import com.alibaba.fastjson.serializer.SerializerFeature;
import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MQVersion.Version;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.common.protocol.header.namesrv.AddWritePermOfBrokerRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.AddWritePermOfBrokerResponseHeader;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.namesrv.NamesrvUtil;
import org.apache.rocketmq.common.namesrv.RegisterBrokerResult;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.RegisterBrokerBody;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.common.protocol.header.GetTopicsByClusterRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.DeleteKVConfigRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.DeleteTopicFromNamesrvRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.GetKVConfigRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.GetKVConfigResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.GetKVListByNamespaceRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.GetRouteInfoRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.PutKVConfigRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.QueryDataVersionRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.QueryDataVersionResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.RegisterBrokerRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.RegisterBrokerResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.UnRegisterBrokerRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.WipeWritePermOfBrokerRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.WipeWritePermOfBrokerResponseHeader;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager;
import org.apache.rocketmq.proxy.common.protocol.NotifyRegisterHeader;
import org.apache.rocketmq.proxy.common.protocol.NotifyRouteInfoGetHeader;
import org.apache.rocketmq.proxy.common.protocol.NotifySwitchHeader;
import org.apache.rocketmq.proxy.common.protocol.TopicRouteTable;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.netty.AsyncNettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode;
import org.apache.rocketmq.proxy.common.protocol.ProxyRequestCode;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public class DefaultRequestProcessor extends AsyncNettyRequestProcessor implements NettyRequestProcessor {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

    protected final NamesrvController namesrvController;

    public DefaultRequestProcessor(NamesrvController namesrvController) {
        this.namesrvController = namesrvController;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {

        if (ctx != null) {
            log.debug("receive request, {} {} {}",
                request.getCode(),
                RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                request);
        }
        Version brokerVersion = MQVersion.value2Version(request.getVersion());
        switch (request.getCode()) {
            case RequestCode.PUT_KV_CONFIG:
                return this.putKVConfig(ctx, request);
            case RequestCode.GET_KV_CONFIG:
                return this.getKVConfig(ctx, request);
            case RequestCode.DELETE_KV_CONFIG:
                return this.deleteKVConfig(ctx, request);
            case RequestCode.QUERY_DATA_VERSION:
                return queryBrokerTopicConfig(ctx, request);
            case RequestCode.REGISTER_BROKER:
                if (brokerVersion.ordinal() >= MQVersion.Version.V3_0_11.ordinal()) {
                    return this.registerBrokerWithFilterServer(ctx, request);
                } else {
                    return this.registerBroker(ctx, request);
                }
            case RequestCode.UNREGISTER_BROKER:
                return this.unregisterBroker(ctx, request);
            case RequestCode.GET_ROUTEINFO_BY_TOPIC:
                return this.getRouteInfoByTopic(ctx, request);
            case RequestCode.GET_BROKER_CLUSTER_INFO:
                return this.getBrokerClusterInfo(ctx, request);
            case RequestCode.WIPE_WRITE_PERM_OF_BROKER:
                return this.wipeWritePermOfBroker(ctx, request);
            case RequestCode.ADD_WRITE_PERM_OF_BROKER:
                return this.addWritePermOfBroker(ctx, request);
            case RequestCode.GET_ALL_TOPIC_LIST_FROM_NAMESERVER:
                return getAllTopicListFromNameserver(ctx, request);
            case RequestCode.DELETE_TOPIC_IN_NAMESRV:
                return deleteTopicInNamesrv(ctx, request);
            case RequestCode.GET_KVLIST_BY_NAMESPACE:
                return this.getKVListByNamespace(ctx, request);
            case RequestCode.GET_TOPICS_BY_CLUSTER:
                return this.getTopicsByCluster(ctx, request);
            case RequestCode.GET_SYSTEM_TOPIC_LIST_FROM_NS:
                return this.getSystemTopicListFromNs(ctx, request);
            case RequestCode.GET_UNIT_TOPIC_LIST:
                return this.getUnitTopicList(ctx, request);
            case RequestCode.GET_HAS_UNIT_SUB_TOPIC_LIST:
                return this.getHasUnitSubTopicList(ctx, request);
            case RequestCode.GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST:
                return this.getHasUnitSubUnUnitTopicList(ctx, request);
            case RequestCode.UPDATE_NAMESRV_CONFIG:
                return this.updateConfig(ctx, request);
            case RequestCode.GET_NAMESRV_CONFIG:
                return this.getConfig(ctx, request);
            case ProxyRequestCode.QUERY_PROXY_DATA_VERSION:
                return queryBrokerTopicConfig(ctx, request, true);
            case ProxyRequestCode.REGISTER_PROXY:
                if (brokerVersion.ordinal() >= MQVersion.Version.V3_0_11.ordinal()) {
                    return this.registerBrokerWithFilterServer(ctx, request, true);
                } else {
                    return this.registerBroker(ctx, request, true);
                }
            case ProxyRequestCode.UNREGISTER_PROXY:
                return this.unregisterBroker(ctx, request, true);
            case ProxyRequestCode.NAMESERVER_SWITCH_TO_BROKER:
                return this.switchToBroker(ctx, request);
            case ProxyRequestCode.NAMESERVER_SWITCH_TO_PROXY:
                return this.switchToProxy(ctx, request);
            case ProxyRequestCode.QUERY_NAMESERVER_TYPE:
                return this.queryNameserverType(ctx, request);
            case ProxyRequestCode.GET_BROKER_ROUTEINFO_BY_TOPIC:
                return this.getRouteInfoByTopic(ctx, request, false);
            case ProxyRequestCode.GET_BROKER_ALL_ROUTEINFO_BY_TOPIC:
                return this.getAllRouteInfoByTopic(ctx, request);
            case ProxyRequestCode.GET_BROKER_BROKER_CLUSTER_INFO:
                return this.getBrokerClusterInfo(ctx, request, false);
            case ProxyRequestCode.GET_BROKER_ALL_TOPIC_LIST_FROM_NAMESERVER:
                return this.getAllTopicListFromNameserver(ctx, request, false);
            case ProxyRequestCode.GET_BROKER_TOPICS_BY_CLUSTER:
                return this.getTopicsByCluster(ctx, request, false);
            case ProxyRequestCode.GET_BROKER_SYSTEM_TOPIC_LIST_FROM_NS:
                return this.getSystemTopicListFromNs(ctx, request, false);
            case ProxyRequestCode.GET_BROKER_UNIT_TOPIC_LIST:
                return this.getUnitTopicList(ctx, request, false);
            case ProxyRequestCode.GET_BROKER_HAS_UNIT_SUB_TOPIC_LIST:
                return this.getHasUnitSubTopicList(ctx, request, false);
            case ProxyRequestCode.GET_BROKER_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST:
                return this.getHasUnitSubUnUnitTopicList(ctx, request, false);
            case ProxyRequestCode.GET_PROXY_ROUTEINFO_BY_TOPIC:
                return this.getRouteInfoByTopic(ctx, request, true);
            case ProxyRequestCode.GET_PROXY_BROKER_CLUSTER_INFO:
                return this.getBrokerClusterInfo(ctx, request, true);
            case ProxyRequestCode.GET_PROXY_ALL_TOPIC_LIST_FROM_NAMESERVER:
                return this.getAllTopicListFromNameserver(ctx, request, true);
            case ProxyRequestCode.GET_PROXY_TOPICS_BY_CLUSTER:
                return this.getTopicsByCluster(ctx, request, true);
            case ProxyRequestCode.GET_PROXY_SYSTEM_TOPIC_LIST_FROM_NS:
                return this.getSystemTopicListFromNs(ctx, request, true);
            case ProxyRequestCode.GET_PROXY_UNIT_TOPIC_LIST:
                return this.getUnitTopicList(ctx, request, true);
            case ProxyRequestCode.GET_PROXY_HAS_UNIT_SUB_TOPIC_LIST:
                return this.getHasUnitSubTopicList(ctx, request, true);
            case ProxyRequestCode.GET_PROXY_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST:
                return this.getHasUnitSubUnUnitTopicList(ctx, request, true);
            default:
                return this.getUnknownCmdResponse(ctx, request);
        }
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    public RemotingCommand putKVConfig(ChannelHandlerContext ignore,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final PutKVConfigRequestHeader requestHeader =
            (PutKVConfigRequestHeader) request.decodeCommandCustomHeader(PutKVConfigRequestHeader.class);

        if (requestHeader.getNamespace() == null || requestHeader.getKey() == null) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("namespace or key is null");
            return response;
        }
        this.namesrvController.getKvConfigManager().putKVConfig(
            requestHeader.getNamespace(),
            requestHeader.getKey(),
            requestHeader.getValue()
        );

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    public RemotingCommand getKVConfig(ChannelHandlerContext ignore,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(GetKVConfigResponseHeader.class);
        final GetKVConfigResponseHeader responseHeader = (GetKVConfigResponseHeader) response.readCustomHeader();
        final GetKVConfigRequestHeader requestHeader =
            (GetKVConfigRequestHeader) request.decodeCommandCustomHeader(GetKVConfigRequestHeader.class);

        String value = this.namesrvController.getKvConfigManager().getKVConfig(
            requestHeader.getNamespace(),
            requestHeader.getKey()
        );

        if (value != null) {
            responseHeader.setValue(value);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return response;
        }

        response.setCode(ResponseCode.QUERY_NOT_FOUND);
        response.setRemark("No config item, Namespace: " + requestHeader.getNamespace() + " Key: " + requestHeader.getKey());
        return response;
    }

    public RemotingCommand deleteKVConfig(ChannelHandlerContext ignore,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final DeleteKVConfigRequestHeader requestHeader =
            (DeleteKVConfigRequestHeader) request.decodeCommandCustomHeader(DeleteKVConfigRequestHeader.class);

        this.namesrvController.getKvConfigManager().deleteKVConfig(
            requestHeader.getNamespace(),
            requestHeader.getKey()
        );

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    public RemotingCommand registerBrokerWithFilterServer(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        return registerBrokerWithFilterServer(ctx, request, false);
    }

    public RemotingCommand registerBrokerWithFilterServer(ChannelHandlerContext ctx, RemotingCommand request,
                                                          boolean isProxy)
        throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(RegisterBrokerResponseHeader.class);
        final RegisterBrokerResponseHeader responseHeader = (RegisterBrokerResponseHeader) response.readCustomHeader();
        final RegisterBrokerRequestHeader requestHeader =
            (RegisterBrokerRequestHeader) request.decodeCommandCustomHeader(RegisterBrokerRequestHeader.class);

        if (!checksum(ctx, request, requestHeader)) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("crc32 not match");
            return response;
        }

        RegisterBrokerBody registerBrokerBody = new RegisterBrokerBody();

        if (request.getBody() != null) {
            try {
                registerBrokerBody = RegisterBrokerBody.decode(request.getBody(), requestHeader.isCompressed());
            } catch (Exception e) {
                throw new RemotingCommandException("Failed to decode RegisterBrokerBody", e);
            }
        } else {
            registerBrokerBody.getTopicConfigSerializeWrapper().getDataVersion().setCounter(new AtomicLong(0));
            registerBrokerBody.getTopicConfigSerializeWrapper().getDataVersion().setTimestamp(0);
        }

        RegisterBrokerResult result = this.getRouteInfoManager(isProxy).registerBroker(
            requestHeader.getClusterName(),
            requestHeader.getBrokerAddr(),
            requestHeader.getBrokerName(),
            requestHeader.getBrokerId(),
            requestHeader.getHaServerAddr(),
            registerBrokerBody.getTopicConfigSerializeWrapper(),
            registerBrokerBody.getFilterServerList(),
            ctx.channel());

        responseHeader.setHaServerAddr(result.getHaServerAddr());
        responseHeader.setMasterAddr(result.getMasterAddr());

        byte[] jsonValue = this.namesrvController.getKvConfigManager().getKVListByNamespace(NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG);
        response.setBody(jsonValue);

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        notifyProxyRegisterBroker(true, isProxy);
        return response;
    }

    private boolean checksum(ChannelHandlerContext ctx,
                             RemotingCommand request,
                             RegisterBrokerRequestHeader requestHeader) {
        if (requestHeader.getBodyCrc32() != 0) {
            final int crc32 = UtilAll.crc32(request.getBody());
            if (crc32 != requestHeader.getBodyCrc32()) {
                log.warn(String.format("receive registerBroker request,crc32 not match,from %s",
                    RemotingHelper.parseChannelRemoteAddr(ctx.channel())));
                return false;
            }
        }
        return true;
    }

    public RemotingCommand queryBrokerTopicConfig(ChannelHandlerContext ctx,
                                                  RemotingCommand request) throws RemotingCommandException {
        return queryBrokerTopicConfig(ctx, request, false);
    }

    public RemotingCommand queryBrokerTopicConfig(ChannelHandlerContext ignore,
                                                  RemotingCommand request,
                                                  boolean isProxy) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(QueryDataVersionResponseHeader.class);
        final QueryDataVersionResponseHeader responseHeader = (QueryDataVersionResponseHeader) response.readCustomHeader();
        final QueryDataVersionRequestHeader requestHeader =
            (QueryDataVersionRequestHeader) request.decodeCommandCustomHeader(QueryDataVersionRequestHeader.class);
        DataVersion dataVersion = DataVersion.decode(request.getBody(), DataVersion.class);

        RouteInfoManager routeInfoManager = this.getRouteInfoManager(false, isProxy);

        Boolean changed = routeInfoManager.isBrokerTopicConfigChanged(requestHeader.getBrokerAddr(), dataVersion);
        if (!changed) {
            routeInfoManager.updateBrokerInfoUpdateTimestamp(requestHeader.getBrokerAddr(), System.currentTimeMillis());
        }

        DataVersion nameSeverDataVersion = routeInfoManager.queryBrokerTopicConfig(requestHeader.getBrokerAddr());
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);

        if (nameSeverDataVersion != null) {
            response.setBody(nameSeverDataVersion.encode());
        }
        responseHeader.setChanged(changed);
        return response;
    }

    public RemotingCommand registerBroker(ChannelHandlerContext ctx,
                                          RemotingCommand request) throws RemotingCommandException {
        return registerBroker(ctx, request, false);
    }

    public RemotingCommand registerBroker(ChannelHandlerContext ctx,
                                          RemotingCommand request,
                                          boolean isProxy) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(RegisterBrokerResponseHeader.class);
        final RegisterBrokerResponseHeader responseHeader = (RegisterBrokerResponseHeader) response.readCustomHeader();
        final RegisterBrokerRequestHeader requestHeader =
                (RegisterBrokerRequestHeader) request.decodeCommandCustomHeader(RegisterBrokerRequestHeader.class);

        if (!checksum(ctx, request, requestHeader)) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("crc32 not match");
            return response;
        }

        TopicConfigSerializeWrapper topicConfigWrapper;
        if (request.getBody() != null) {
            topicConfigWrapper = TopicConfigSerializeWrapper.decode(request.getBody(), TopicConfigSerializeWrapper.class);
        } else {
            topicConfigWrapper = new TopicConfigSerializeWrapper();
            topicConfigWrapper.getDataVersion().setCounter(new AtomicLong(0));
            topicConfigWrapper.getDataVersion().setTimestamp(0);
        }

        RegisterBrokerResult result = this.getRouteInfoManager(isProxy).registerBroker(
                requestHeader.getClusterName(),
                requestHeader.getBrokerAddr(),
                requestHeader.getBrokerName(),
                requestHeader.getBrokerId(),
                requestHeader.getHaServerAddr(),
                topicConfigWrapper,
                null,
                ctx.channel()
        );

        responseHeader.setHaServerAddr(result.getHaServerAddr());
        responseHeader.setMasterAddr(result.getMasterAddr());

        byte[] jsonValue = this.namesrvController.getKvConfigManager().getKVListByNamespace(NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG);
        response.setBody(jsonValue);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        notifyProxyRegisterBroker(true, isProxy);
        return response;
    }

    public RemotingCommand unregisterBroker(ChannelHandlerContext ctx,
                                            RemotingCommand request) throws RemotingCommandException {
        return unregisterBroker(ctx, request, false);
    }

    public RemotingCommand unregisterBroker(ChannelHandlerContext ignore,
                                            RemotingCommand request,
                                            boolean isProxy) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final UnRegisterBrokerRequestHeader requestHeader =
            (UnRegisterBrokerRequestHeader) request.decodeCommandCustomHeader(UnRegisterBrokerRequestHeader.class);

        this.getRouteInfoManager(isProxy).unregisterBroker(
            requestHeader.getClusterName(),
            requestHeader.getBrokerAddr(),
            requestHeader.getBrokerName(),
            requestHeader.getBrokerId());

        notifyProxyRegisterBroker(false, isProxy);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    public RemotingCommand getRouteInfoByTopic(ChannelHandlerContext ctx,
                                               RemotingCommand request) throws RemotingCommandException {
        return this.getRouteInfoByTopic(ctx, request, true, true);
    }

    public RemotingCommand getRouteInfoByTopic(ChannelHandlerContext ctx,
                                               RemotingCommand request,
                                               boolean isProxy) throws RemotingCommandException {
        return this.getRouteInfoByTopic(ctx, request, false, isProxy);
    }

    public RemotingCommand getAllRouteInfoByTopic(ChannelHandlerContext ignore, RemotingCommand ignore0) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        RouteInfoManager routeInfo = this.getRouteInfoManager(false, false);
        TopicRouteTable routeTable = new TopicRouteTable();

        TopicList topicList = routeInfo.getAllTopicList();

        if (topicList == null || topicList.getTopicList() == null || topicList.getTopicList().isEmpty()) {
            response.setCode(ResponseCode.TOPIC_NOT_EXIST);
            response.setRemark("No topic route info in name server" + FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL));
            return response;
        }

        Map<String, TopicRouteData> routeData = new HashMap<>();
        for (String topic : topicList.getTopicList()) {
            TopicRouteData topicRouteData = routeInfo.pickupTopicRouteData(topic);
            if (topic != null && topicRouteData != null) {
                routeData.put(topic, topicRouteData);
            }
        }

        if (!routeData.isEmpty()) {
            routeTable.setTopicRouteData(routeData);
            byte[] content;
            content = routeTable.encode(SerializerFeature.BrowserCompatible,
                    SerializerFeature.QuoteFieldNames, SerializerFeature.SkipTransientField,
                    SerializerFeature.MapSortField);
            response.setBody(content);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return response;
        }

        response.setCode(ResponseCode.TOPIC_NOT_EXIST);
        response.setRemark("No topic route info in name server" + FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL));
        return response;
    }

    public RemotingCommand getRouteInfoByTopic(ChannelHandlerContext ignore,
                                               RemotingCommand request,
                                               boolean current,
                                               boolean isProxy) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final GetRouteInfoRequestHeader requestHeader =
            (GetRouteInfoRequestHeader) request.decodeCommandCustomHeader(GetRouteInfoRequestHeader.class);

        TopicRouteData topicRouteData = this.getRouteInfoManager(current, isProxy).pickupTopicRouteData(requestHeader.getTopic());

        if (topicRouteData != null) {
            if (this.namesrvController.getNamesrvConfig().isOrderMessageEnable()) {
                String orderTopicConf =
                    this.namesrvController.getKvConfigManager().getKVConfig(NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG,
                        requestHeader.getTopic());
                topicRouteData.setOrderTopicConf(orderTopicConf);
            }

            byte[] content;
            Boolean standardJsonOnly = requestHeader.getAcceptStandardJsonOnly();
            if (request.getVersion() >= Version.V4_9_4.ordinal() || (null != standardJsonOnly && standardJsonOnly)) {
                content = topicRouteData.encode(SerializerFeature.BrowserCompatible,
                    SerializerFeature.QuoteFieldNames, SerializerFeature.SkipTransientField,
                    SerializerFeature.MapSortField);
            } else {
                content = RemotingSerializable.encode(topicRouteData);
            }

            notifyRouteGet(requestHeader.getTopic(), this.namesrvController.isRouteInfoProxy());

            response.setBody(content);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return response;
        }

        response.setCode(ResponseCode.TOPIC_NOT_EXIST);
        response.setRemark("No topic route info in name server for the topic: " + requestHeader.getTopic()
            + FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL));
        return response;
    }

    private RemotingCommand getBrokerClusterInfo(ChannelHandlerContext ctx,
                                                 RemotingCommand request) {
        return this.getBrokerClusterInfo(ctx, request, true, true);
    }

    private RemotingCommand getBrokerClusterInfo(ChannelHandlerContext ctx,
                                                 RemotingCommand request,
                                                 boolean isProxy) {
        return this.getBrokerClusterInfo(ctx, request, false, isProxy);
    }

    private RemotingCommand getBrokerClusterInfo(ChannelHandlerContext ignore,
                                                 RemotingCommand ignore0,
                                                 boolean current,
                                                 boolean isProxy) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        ClusterInfo clusterInfo = this.getRouteInfoManager(current, isProxy).getAllClusterInfo();
        byte[] content = clusterInfo.encode();
        response.setBody(content);

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand wipeWritePermOfBroker(ChannelHandlerContext ctx,
                                                  RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(WipeWritePermOfBrokerResponseHeader.class);
        final WipeWritePermOfBrokerResponseHeader responseHeader = (WipeWritePermOfBrokerResponseHeader) response.readCustomHeader();
        final WipeWritePermOfBrokerRequestHeader requestHeader =
            (WipeWritePermOfBrokerRequestHeader) request.decodeCommandCustomHeader(WipeWritePermOfBrokerRequestHeader.class);

        int wipeTopicCnt = this.namesrvController.getBrokerRouteInfoManager().wipeWritePermOfBrokerByLock(requestHeader.getBrokerName());
        int wipeTopicCnt2 = this.namesrvController.getProxyRouteInfoManager().wipeWritePermOfBrokerByLock(requestHeader.getBrokerName());

        if (ctx != null) {
            log.info("wipe write perm of broker[{}], client: {}, {} {}",
                    requestHeader.getBrokerName(),
                    RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                    wipeTopicCnt, wipeTopicCnt2);
        }

        responseHeader.setWipeTopicCount(wipeTopicCnt);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand addWritePermOfBroker(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(AddWritePermOfBrokerResponseHeader.class);
        final AddWritePermOfBrokerResponseHeader responseHeader = (AddWritePermOfBrokerResponseHeader) response.readCustomHeader();
        final AddWritePermOfBrokerRequestHeader requestHeader = (AddWritePermOfBrokerRequestHeader) request.decodeCommandCustomHeader(AddWritePermOfBrokerRequestHeader.class);

        int addTopicCnt = this.namesrvController.getBrokerRouteInfoManager().addWritePermOfBrokerByLock(requestHeader.getBrokerName());
        int addTopicCnt2 = this.namesrvController.getProxyRouteInfoManager().addWritePermOfBrokerByLock(requestHeader.getBrokerName());

        log.info("add write perm of broker[{}], client: {}, {} {}",
                requestHeader.getBrokerName(),
                RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                addTopicCnt,
                addTopicCnt2);

        responseHeader.setAddTopicCount(addTopicCnt);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getAllTopicListFromNameserver(ChannelHandlerContext ctx,
                                                          RemotingCommand request) {
        return this.getAllTopicListFromNameserver(ctx, request, true, true);
    }

    private RemotingCommand getAllTopicListFromNameserver(ChannelHandlerContext ctx,
                                                          RemotingCommand request,
                                                          boolean isProxy) {
        return this.getAllTopicListFromNameserver(ctx, request, false, isProxy);
    }

    private RemotingCommand getAllTopicListFromNameserver(ChannelHandlerContext ignore,
                                                          RemotingCommand ignore0,
                                                          boolean current,
                                                          boolean isProxy) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        TopicList allTopicList = this.getRouteInfoManager(current, isProxy).getAllTopicList();
        byte[] body = allTopicList.encode();

        response.setBody(body);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand deleteTopicInNamesrv(ChannelHandlerContext ignore,
                                                 RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final DeleteTopicFromNamesrvRequestHeader requestHeader =
            (DeleteTopicFromNamesrvRequestHeader) request.decodeCommandCustomHeader(DeleteTopicFromNamesrvRequestHeader.class);

        if (requestHeader.getClusterName() != null
            && !requestHeader.getClusterName().isEmpty()) {
            this.namesrvController.getBrokerRouteInfoManager().deleteTopic(requestHeader.getTopic(), requestHeader.getClusterName());
            this.namesrvController.getProxyRouteInfoManager().deleteTopic(requestHeader.getTopic(), requestHeader.getClusterName());
        } else {
            this.namesrvController.getBrokerRouteInfoManager().deleteTopic(requestHeader.getTopic());
            this.namesrvController.getProxyRouteInfoManager().deleteTopic(requestHeader.getTopic());
        }

        this.namesrvController.getProxyRouteInfoManager().getAllClusterInfo().getBrokerAddrTable().values()
                .forEach(brokerData -> {
                    try {
                        RemotingHelper.invokeSync(brokerData.selectBrokerAddr(), request, 5000);
                    } catch (InterruptedException | RemotingConnectException | RemotingSendRequestException |
                             RemotingTimeoutException | RemotingCommandException e) {
                        throw new RuntimeException(e);
                    }
                });

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getKVListByNamespace(ChannelHandlerContext ignore,
                                                 RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final GetKVListByNamespaceRequestHeader requestHeader =
            (GetKVListByNamespaceRequestHeader) request.decodeCommandCustomHeader(GetKVListByNamespaceRequestHeader.class);

        byte[] jsonValue = this.namesrvController.getKvConfigManager().getKVListByNamespace(
            requestHeader.getNamespace());
        if (null != jsonValue) {
            response.setBody(jsonValue);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return response;
        }

        response.setCode(ResponseCode.QUERY_NOT_FOUND);
        response.setRemark("No config item, Namespace: " + requestHeader.getNamespace());
        return response;
    }

    private RemotingCommand getTopicsByCluster(ChannelHandlerContext ctx,
                                               RemotingCommand request) throws RemotingCommandException {
        return getTopicsByCluster(ctx, request, true, true);
    }

    private RemotingCommand getTopicsByCluster(ChannelHandlerContext ctx,
                                               RemotingCommand request,
                                               boolean isProxy) throws RemotingCommandException {
        return getTopicsByCluster(ctx, request, false, isProxy);

    }
    private RemotingCommand getTopicsByCluster(ChannelHandlerContext ignore,
                                               RemotingCommand request,
                                               boolean current,
                                               boolean isProxy) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final GetTopicsByClusterRequestHeader requestHeader =
            (GetTopicsByClusterRequestHeader) request.decodeCommandCustomHeader(GetTopicsByClusterRequestHeader.class);

        TopicList topicsByCluster = this.getRouteInfoManager(current, isProxy).getTopicsByCluster(requestHeader.getCluster());
        byte[] body = topicsByCluster.encode();

        response.setBody(body);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getSystemTopicListFromNs(ChannelHandlerContext ctx,
                                                     RemotingCommand request) {
        return getSystemTopicListFromNs(ctx, request, true, true);
    }

    private RemotingCommand getSystemTopicListFromNs(ChannelHandlerContext ctx,
                                                     RemotingCommand request,
                                                     boolean isProxy) {
        return getSystemTopicListFromNs(ctx, request, false, isProxy);
    }

    private RemotingCommand getSystemTopicListFromNs(ChannelHandlerContext ignore,
                                                     RemotingCommand ignore0,
                                                     boolean current,
                                                     boolean isProxy) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        TopicList systemTopicList = this.getRouteInfoManager(current, isProxy).getSystemTopicList();
        byte[] body = systemTopicList.encode();

        response.setBody(body);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getUnitTopicList(ChannelHandlerContext ctx,
                                             RemotingCommand request) {
        return this.getUnitTopicList(ctx, request, true, true);
    }
    private RemotingCommand getUnitTopicList(ChannelHandlerContext ctx,
                                             RemotingCommand request,
                                             boolean isProxy) {
        return this.getUnitTopicList(ctx, request, false, isProxy);
    }

    private RemotingCommand getUnitTopicList(ChannelHandlerContext ignore,
                                             RemotingCommand ignore0,
                                             boolean current,
                                             boolean isProxy) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        TopicList unitTopics = this.getRouteInfoManager(current, isProxy).getUnitTopics();
        byte[] body = unitTopics.encode();

        response.setBody(body);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getHasUnitSubTopicList(ChannelHandlerContext ctx,
                                                   RemotingCommand request) {
        return getHasUnitSubTopicList(ctx, request, true, true);
    }

    private RemotingCommand getHasUnitSubTopicList(ChannelHandlerContext ctx,
                                                   RemotingCommand request,
                                                   boolean isProxy) {
        return getHasUnitSubTopicList(ctx, request, false, isProxy);
    }

    private RemotingCommand getHasUnitSubTopicList(ChannelHandlerContext ignore,
                                                   RemotingCommand ignore0,
                                                   boolean current,
                                                   boolean isProxy) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        TopicList hasUnitSubTopicList = this.getRouteInfoManager(current, isProxy).getHasUnitSubTopicList();
        byte[] body = hasUnitSubTopicList.encode();

        response.setBody(body);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getHasUnitSubUnUnitTopicList(ChannelHandlerContext ctx, RemotingCommand request) {
        return getHasUnitSubUnUnitTopicList(ctx, request, true, true);
    }

    private RemotingCommand getHasUnitSubUnUnitTopicList(ChannelHandlerContext ctx,
                                                         RemotingCommand request,
                                                         boolean proxy) {
        return getHasUnitSubUnUnitTopicList(ctx, request, false, proxy);
    }

    private RemotingCommand getHasUnitSubUnUnitTopicList(ChannelHandlerContext ignore,
                                                         RemotingCommand ignore0,
                                                         boolean current,
                                                         boolean isProxy) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        TopicList hasUnitSubUnUnitTopicList = this.getRouteInfoManager(current, isProxy).getHasUnitSubUnUnitTopicList();
        byte[] body = hasUnitSubUnUnitTopicList.encode();

        response.setBody(body);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand updateConfig(ChannelHandlerContext ctx, RemotingCommand request) {
        if (ctx != null) {
            log.info("updateConfig called by {}", RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
        }

        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        byte[] body = request.getBody();
        if (body != null) {
            String bodyStr;
            try {
                bodyStr = new String(body, MixAll.DEFAULT_CHARSET);
            } catch (UnsupportedEncodingException e) {
                log.error("updateConfig byte array to string error: ", e);
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }

            Properties properties = MixAll.string2Properties(bodyStr);
            if (properties == null) {
                log.error("updateConfig MixAll.string2Properties error {}", bodyStr);
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("string2Properties error");
                return response;
            }

            if (properties.containsKey("kvConfigPath") || properties.containsKey("configStorePath")) {
                response.setCode(ResponseCode.NO_PERMISSION);
                response.setRemark("Can not update config path");
                return response;
            }

            this.namesrvController.getConfiguration().update(properties);
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getConfig(ChannelHandlerContext ignore, RemotingCommand ignore0) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        String content = this.namesrvController.getConfiguration().getAllConfigsFormatString();
        if (content != null && !content.isEmpty()) {
            try {
                response.setBody(content.getBytes(MixAll.DEFAULT_CHARSET));
            } catch (UnsupportedEncodingException e) {
                log.error("getConfig error, ", e);
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RouteInfoManager getRouteInfoManager(boolean proxy) {
        return getRouteInfoManager(false, proxy);
    }

    private RouteInfoManager getRouteInfoManager(boolean current, boolean proxy) {
        if (current) {
            return this.namesrvController.getRouteInfoManager();
        } else if (proxy) {
            return this.namesrvController.getProxyRouteInfoManager();
        } else {
            return this.namesrvController.getBrokerRouteInfoManager();
        }
    }

    private RemotingCommand switchToBroker(ChannelHandlerContext ignore, RemotingCommand ignore0) {
        this.namesrvController.changeRouteInfoManagerToBroker();
        notifyProxySwitch(false);
        return RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "");
    }

    private RemotingCommand switchToProxy(ChannelHandlerContext ignore, RemotingCommand ignore0) {
        this.namesrvController.changeRouteInfoManagerToProxy();
        notifyProxySwitch(true);
        return RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "");
    }

    private RemotingCommand queryNameserverType(ChannelHandlerContext ignore, RemotingCommand ignore0) {
        return RemotingCommand.createResponseCommand(ResponseCode.SUCCESS,
                this.namesrvController.isRouteInfoProxy() ? "PROXY" : "BROKER");
    }

    private RemotingCommand getUnknownCmdResponse(ChannelHandlerContext ignore, RemotingCommand request) {
        String error = " request type " + request.getCode() + " not supported";
        return RemotingCommand.createResponseCommand(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED, error);
    }

    private void notifyProxyRegisterBroker(boolean isReg, boolean isProxy) {
        notifyAllProxy(RemotingCommand.createRequestCommand(
                ProxyRequestCode.NOTIFY_PROXY_REGISTER_BROKER_OR_PROXY,
                new NotifyRegisterHeader(isReg, isProxy)
        ));
    }

    private void notifyProxySwitch(boolean proxy) {
        notifyAllProxy(RemotingCommand.createRequestCommand(
                ProxyRequestCode.NOTIFY_PROXY_BROKER_SWITCH,
                new NotifySwitchHeader(proxy)
        ));
    }

    private void notifyRouteGet(String topic, boolean proxy) {
        notifyAllProxy(RemotingCommand.createRequestCommand(
                ProxyRequestCode.NOTIFY_PROXY_ROUTE_INFO_GET,
                new NotifyRouteInfoGetHeader(topic, proxy)
        ));
    }

    private void notifyAllProxy(RemotingCommand request) {
        ClusterInfo proxyClusterInfo = this.namesrvController.getProxyRouteInfoManager().getAllClusterInfo();

        if (proxyClusterInfo == null || proxyClusterInfo.getBrokerAddrTable() == null) {
            return;
        }
        for (BrokerData brokerData : proxyClusterInfo.getBrokerAddrTable().values()) {
            try {
                this.namesrvController.getRemotingClient().invokeOneway(
                        brokerData.selectBrokerAddr(),
                        request,
                        5000
                );
            } catch (InterruptedException | RemotingConnectException | RemotingSendRequestException |
                     RemotingTimeoutException | RemotingTooMuchRequestException e) {
                log.error("notify proxy error", e);
            }
        }
    }
}
