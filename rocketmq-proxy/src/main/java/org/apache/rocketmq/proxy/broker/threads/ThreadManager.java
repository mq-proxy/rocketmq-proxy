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
package org.apache.rocketmq.proxy.broker.threads;

import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * author: G
 * create: 2024-05-10
 */
public class ThreadManager {

    private static ThreadManager threadManager;

    private static Executor asyncBrokerExecutor;
    private ThreadManager() {
        int processors = Runtime.getRuntime().availableProcessors();
        int standardNumber = processors < 4 ? 8 : processors * 3;
        asyncBrokerExecutor = new ThreadPoolExecutor(standardNumber, standardNumber, 60, TimeUnit.SECONDS,
                new LinkedBlockingDeque<>(5000));
    }

    public static ThreadManager getThreadPool() {
        if (threadManager == null) {
            synchronized (ThreadManager.class) {
                if (threadManager == null) {
                    threadManager = new ThreadManager();
                }
            }
        }

        return threadManager;
    }

    public static Executor getAsyncBrokerExecutor() {
        return asyncBrokerExecutor;
    }
}
