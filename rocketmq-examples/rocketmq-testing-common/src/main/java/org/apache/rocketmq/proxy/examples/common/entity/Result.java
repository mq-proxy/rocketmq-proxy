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
package org.apache.rocketmq.proxy.examples.common.entity;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

public class Result {
    private final Map<String, BitSet> bitSetMap = new HashMap<>();

    private final BitSet bitSetDefault = new BitSet();

    public synchronized boolean exists(String id, int n) {
        return bitSetMap.getOrDefault(id, bitSetDefault).get(n);
    }

    public synchronized void set(String id, int n) {
        bitSetMap.computeIfAbsent(id, k -> new BitSet()).set(n);
    }

    public synchronized int next(String id) {
        return bitSetMap.getOrDefault(id, bitSetDefault).nextClearBit(0);
    }
}
