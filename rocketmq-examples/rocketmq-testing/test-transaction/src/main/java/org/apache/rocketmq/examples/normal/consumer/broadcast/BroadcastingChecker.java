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
package org.apache.rocketmq.examples.normal.consumer.broadcast;

import org.apache.rocketmq.examples.normal.TransactionTest;
import org.apache.rocketmq.proxy.examples.common.entity.Msg;
import org.apache.rocketmq.proxy.examples.common.result.AbstractChecker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BroadcastingChecker extends AbstractChecker {
    public static BroadcastingChecker resultChecker = new BroadcastingChecker();
    private final Logger log = LoggerFactory.getLogger(BroadcastingChecker.class);

    public static BroadcastingChecker getInstance() {
        return resultChecker;
    }

    private BroadcastingChecker() {
    }

    @Override
    protected boolean check() {
        int nextAIndex = next("A" + TransactionTest.ID);
        int nextBIndex = next("B" + TransactionTest.ID);
        log.info("broadcast next, A:{}, B:{}", nextAIndex, nextBIndex);
        return nextAIndex == TransactionTest.COUNT && nextBIndex == TransactionTest.COUNT;
    }

    public void recordA(Msg msg) {
        record("A" + msg.getId(), msg.getIndex());
    }

    public void recordB(Msg msg) {
        record("B" + msg.getId(), msg.getIndex());
    }
}
