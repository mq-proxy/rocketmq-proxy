package org.apache.rocketmq.proxy.broker.processor.client;

import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.proxy.broker.client.net.Broker2Client;
import org.apache.rocketmq.proxy.broker.processor.AdminProcessTestBase;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.InetSocketAddress;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;

@RunWith(MockitoJUnitRunner.Silent.class)
public class ClientProcessorTest extends AdminProcessTestBase {

    private ClientProcessor clientProcessor;
    @Mock
    private RemotingClient remotingClient;
    @Mock
    private Broker2Client broker2Client;
    @Before
    public void setUp() throws Exception {
        doNothing().when(broker2Client).checkProducerTransactionState(anyString(), any(), any(), any());
        doReturn(broker2Client).when(brokerController).getBroker2Client();
        clientProcessor = new ClientProcessor(this.brokerController);
        processor = clientProcessor;
    }

    @Test
    public void testProcessCheckTransactionStat() throws Exception {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CHECK_TRANSACTION_STATE, null);
        request.setBody(buildMessage());
        RemotingCommand response = processRequest(request);
        assertThat(response).isNull();
    }

    @Test
    public void register() {
        clientProcessor.register(remotingClient, null);
    }

    private byte[] buildMessage() throws Exception{
        MessageExt messageExt = new MessageExt();
        messageExt.setTopic("topic");
        messageExt.setTags("tag");
        messageExt.setKeys("keys");
        messageExt.setBornHost(new InetSocketAddress("127.0.0.1", 19876));
        messageExt.setStoreHost(new InetSocketAddress("127.0.0.1", 29876));
        messageExt.getProperties().put("PGROUP", "test");
        messageExt.setBody("hello world".getBytes());
        return MessageDecoder.encode(messageExt, false);
    }
}