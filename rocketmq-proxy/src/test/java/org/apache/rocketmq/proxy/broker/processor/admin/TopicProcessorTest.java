package org.apache.rocketmq.proxy.broker.processor.admin;

import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.common.protocol.header.CreateTopicRequestHeader;
import org.apache.rocketmq.proxy.broker.processor.AdminProcessTestBase;
import org.apache.rocketmq.proxy.broker.topic.TopicConfigManager;
import org.apache.rocketmq.proxy.proxy.Proxy;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;

@RunWith(MockitoJUnitRunner.class)
public class TopicProcessorTest extends AdminProcessTestBase {

    @Before
    public void setUp() throws Exception {
        processor = new TopicProcessor(brokerController);
    }

    @Test
    public void testProcessUpdateAndCreateTopic() throws Exception {
        try (MockedStatic<Proxy> mockedStatic = mockStatic(Proxy.class)) {
            mockProxy(mockedStatic, proxyName, brokerAddr);
            doMockTopicConfig();

            CreateTopicRequestHeader requestHeader = new CreateTopicRequestHeader();
            requestHeader.setTopic(topic);
            requestHeader.setReadQueueNums(4);
            requestHeader.setWriteQueueNums(4);
            RemotingCommand request = RemotingCommand.createRequestCommand(
                    RequestCode.UPDATE_AND_CREATE_TOPIC, requestHeader);
            RemotingCommand response = processRequest(request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        }
    }

    @Test
    public void testProcessDeleteTopicInBroker() throws Exception {
        try (MockedStatic<Proxy> mockedStatic = mockStatic(Proxy.class)) {
            mockProxy(mockedStatic, proxyName, brokerAddr);
            doMockTopicConfig();

            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_TOPIC_IN_BROKER, null);
            RemotingCommand response = processRequest(request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        }
    }

    @Test
    public void testProcessGetAllTopicConfig() throws Exception {
        try (MockedStatic<Proxy> mockedStatic = mockStatic(Proxy.class)) {
            mockProxy(mockedStatic, proxyName, brokerAddr);
            doMockTopicConfigManager();

            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_TOPIC_CONFIG, null);
            RemotingCommand response = processRequest(request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);

            TopicConfigManager topicConfigManager = new TopicConfigManager();
            topicConfigManager.decode(new String(response.getBody()));
            assertThat(topicConfigManager.getTopicConfigTable()).isNotEmpty();
        }
    }

    @Test
    public void testProcessGetSystemTopicListFromBroker() throws Exception {
        try (MockedStatic<Proxy> mockedStatic = mockStatic(Proxy.class)) {
            mockProxy(mockedStatic, proxyName, brokerAddr);

            doMockTopicList();
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_SYSTEM_TOPIC_LIST_FROM_BROKER, null);
            RemotingCommand response = processRequest(request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);

            TopicList topicList = TopicList.decode(response.getBody(), TopicList.class);
            assertThat(topicList.getTopicList()).isNotEmpty();
        }
    }

    private void doMockTopicList() throws Exception {
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "");
        TopicList topicList = new TopicList();
        topicList.setBrokerAddr(brokerAddr);
        topicList.setTopicList(new HashSet<>(Arrays.asList("a", "b")));
        response.setBody(topicList.encode());
        doMockClusterMapping(response);
    }

    private void doMockTopicConfig() throws Exception {
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "");
        doMockClusterMapping(response);
    }

    private void doMockTopicConfigManager() throws Exception {
        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setTopicName(topic);
        topicConfig.setReadQueueNums(4);
        topicConfig.setWriteQueueNums(4);
        topicConfig.setPerm(6);

        TopicConfigManager topicConfigManager = spy(new TopicConfigManager());
        doNothing().when(topicConfigManager).persist();
        topicConfigManager.updateTopicConfig(topicConfig);

        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        response.setBody(topicConfigManager.encode().getBytes(StandardCharsets.UTF_8));
        doMockClusterMapping(response);
    }
}
