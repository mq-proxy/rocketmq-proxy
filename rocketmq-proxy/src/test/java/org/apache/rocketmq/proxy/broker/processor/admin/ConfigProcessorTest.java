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

import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.proxy.broker.processor.AdminProcessTestBase;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class ConfigProcessorTest extends AdminProcessTestBase {

    @Before
    public void setUp() throws Exception {
        this.processor = new ConfigProcessor(this.brokerController);
    }

    @Test
    public void processRequestGetBrokerConfig() throws Exception {
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CONFIG, null);
        RemotingCommand response = processRequest(remotingCommand);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void processRequestUpdateBrokerConfig() throws Exception {
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.UPDATE_BROKER_CONFIG, null);
        RemotingCommand response = processRequest(remotingCommand);
        assertThat(response.getCode()).isEqualTo(ResponseCode.REQUEST_CODE_NOT_SUPPORTED);
    }

    @Test
    public void processRequestUpdateAclConfig() throws Exception {
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_CREATE_ACL_CONFIG, null);
        RemotingCommand response = processRequest(remotingCommand);
        assertThat(response.getCode()).isEqualTo(ResponseCode.REQUEST_CODE_NOT_SUPPORTED);
    }

    @Test
    public void processRequestDeleteAclConfig() throws Exception {
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.DELETE_ACL_CONFIG, null);
        RemotingCommand response = processRequest(remotingCommand);
        assertThat(response.getCode()).isEqualTo(ResponseCode.REQUEST_CODE_NOT_SUPPORTED);
    }

    @Test
    public void processRequestGetAclConfig() throws Exception {
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CLUSTER_ACL_CONFIG, null);
        RemotingCommand response = processRequest(remotingCommand);
        assertThat(response.getCode()).isEqualTo(ResponseCode.REQUEST_CODE_NOT_SUPPORTED);
    }

    @Test
    public void processRequestGetAclInfo() throws Exception {
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CLUSTER_ACL_INFO, null);
        RemotingCommand response = processRequest(remotingCommand);
        assertThat(response.getCode()).isEqualTo(ResponseCode.REQUEST_CODE_NOT_SUPPORTED);
    }

    @Test
    public void processRequestUpdateGlobalWhiteAddrsConfig() throws Exception {
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.UPDATE_GLOBAL_WHITE_ADDRS_CONFIG, null);
        RemotingCommand response = processRequest(remotingCommand);
        assertThat(response.getCode()).isEqualTo(ResponseCode.REQUEST_CODE_NOT_SUPPORTED);
    }
}