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
package org.apache.rocketmq.proxy.examples.common.client;

import org.apache.rocketmq.proxy.examples.common.tools.EnvTool;

public class ProducerConstants {
    // Constants
    public static final String PRODUCER_GROUP =
            EnvTool.envWithDefault("rocketmq.producer_group", "ROCKETMQ_PRODUCER_GROUP", "proxy_testing_group");
    public static final String MSG_TAG =
            EnvTool.envWithDefault("rocketmq.msg_tag", "ROCKETMQ_TAG", "normal");
    public static final String MSG_PREFIX =
            EnvTool.envWithDefault("rocketmq.msg_prefix", "ROCKETMQ_MSG", "normal");
    public static final String MSG_KEY =
            EnvTool.envWithDefault("rocketmq.msg_key","ROCKETMQ_KEY", "");
    public static final boolean TRANSACTION =
            EnvTool.envWithDefault("rocketmq.transaction", "ROCKETMQ_TRANSACTION", false);
}
