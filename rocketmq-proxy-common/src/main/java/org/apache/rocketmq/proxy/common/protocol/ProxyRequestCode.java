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
package org.apache.rocketmq.proxy.common.protocol;

public class ProxyRequestCode {
    // register & unregister
    public static final int REGISTER_PROXY = 226120001;
    public static final int UNREGISTER_PROXY = 226120002;
    public static final int QUERY_PROXY_DATA_VERSION = 226120003;

    // switch commands
    public static final int NAMESERVER_SWITCH_TO_BROKER = 226120101;
    public static final int NAMESERVER_SWITCH_TO_PROXY = 226120102;

    public static final int QUERY_NAMESERVER_TYPE = 226120201;

    // query broker info
    public static final int GET_BROKER_ROUTEINFO_BY_TOPIC = 226120401;
    public static final int GET_BROKER_BROKER_CLUSTER_INFO = 226120402;
    public static final int GET_BROKER_ALL_TOPIC_LIST_FROM_NAMESERVER  = 226120403;
    public static final int GET_BROKER_TOPICS_BY_CLUSTER = 226120404;
    public static final int GET_BROKER_SYSTEM_TOPIC_LIST_FROM_NS = 226120405;
    public static final int GET_BROKER_UNIT_TOPIC_LIST = 226120406;
    public static final int GET_BROKER_HAS_UNIT_SUB_TOPIC_LIST = 226120407;
    public static final int GET_BROKER_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST = 226120408;
    public static final int GET_BROKER_ALL_ROUTEINFO_BY_TOPIC = 226120411;


    // query proxy info
    public static final int GET_PROXY_ROUTEINFO_BY_TOPIC = 226120501;
    public static final int GET_PROXY_BROKER_CLUSTER_INFO = 226120502;
    public static final int GET_PROXY_ALL_TOPIC_LIST_FROM_NAMESERVER  = 226120503;
    public static final int GET_PROXY_TOPICS_BY_CLUSTER = 226120504;
    public static final int GET_PROXY_SYSTEM_TOPIC_LIST_FROM_NS = 226120505;
    public static final int GET_PROXY_UNIT_TOPIC_LIST = 226120506;
    public static final int GET_PROXY_HAS_UNIT_SUB_TOPIC_LIST = 226120507;
    public static final int GET_PROXY_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST = 226120508;

    // TODO  notify proxy
    public static final int NOTIFY_PROXY_REGISTER_BROKER_OR_PROXY = 226120601;
    public static final int NOTIFY_PROXY_BROKER_SWITCH = 226120602;
    public static final int NOTIFY_PROXY_ROUTE_INFO_GET = 226120603;

    public static final int UNREGISTER_CLIENT_BY_CLIENTID = 226123501;
}
