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

import javax.annotation.Nonnull;
import java.util.Comparator;
import java.util.Objects;

public class BrokerQueue implements Comparable<BrokerQueue> {
    private final String brokerAddr;
    private final int queueId;

    public BrokerQueue(String brokerAddr, int queueId) {
        this.brokerAddr = brokerAddr;
        this.queueId = queueId;
    }

    public String getBrokerAddr() {
        return brokerAddr;
    }

    public int getQueueId() {
        return queueId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BrokerQueue that = (BrokerQueue) o;
        return queueId == that.queueId && Objects.equals(brokerAddr, that.brokerAddr);
    }

    @Override
    public int hashCode() {
        return Objects.hash(brokerAddr, queueId);
    }

    @Override
    public int compareTo(@Nonnull BrokerQueue o) {
        return Objects.compare(this, o, Comparator.comparing((BrokerQueue a) -> a.brokerAddr)
                .thenComparingInt(BrokerQueue::getQueueId));
    }

    @Override
    public String toString() {
        return "[" + brokerAddr + ":" + queueId + "]";
    }
}
