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
package org.apache.rocketmq.proxy.proxy;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * author: G
 * create: 2024-05-17
 */
public class HeartBeatManager {

    private final ConcurrentMap<String, Boolean> unregisterClientRecordMap = new ConcurrentHashMap<>(16);

    /**
     * Record the first heartbeat after switching from broker to proxy,
     * cause the client will neither close the channel to broker, nor unregister its consumer data from broker
     *
     * @param clientId
     * @return
     */
    public boolean checkIfUnregisterInBroker(String clientId) {
        if (unregisterClientRecordMap.containsKey(clientId)) {
            return false;
        }
        unregisterClientRecordMap.put(clientId, false);
        return true;
    }

    /**
     * clean this map when switch proxy to broker
     */
    public synchronized void resetRecord() {
        unregisterClientRecordMap.clear();
    }

}
