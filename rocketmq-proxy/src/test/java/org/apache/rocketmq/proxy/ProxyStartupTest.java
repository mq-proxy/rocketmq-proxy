/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.proxy;

import java.util.Properties;

import org.apache.commons.cli.Options;
import org.apache.rocketmq.proxy.controller.Controller;
import org.apache.rocketmq.proxy.controller.ProxyController;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ProxyStartupTest {

    @Mock
    private ProxyController proxyController;
    @Mock
    private Options options;

    @Before
    public void setUp() {
        Mockito.when(proxyController.initialize()).thenReturn(true);
    }

    @Test
    public void testStart() throws Exception {
        Controller controller = ProxyStartup.start(proxyController);
        Assert.assertNotNull(controller);
    }

    @Test
    public void testShutdown() {
        ProxyStartup.shutdown(proxyController);
        Mockito.verify(proxyController).shutdown();
    }

    @Test
    public void testBuildCommandlineOptions() {
        Options options = ProxyStartup.buildCommandlineOptions(this.options);
        Assert.assertNotNull(options);
    }

    @Test
    public void testGetProperties() {
        Properties properties = ProxyStartup.getProperties();
        Assert.assertNull(properties);
    }
}
