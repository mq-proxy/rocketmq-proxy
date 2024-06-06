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
package org.apache.rocketmq.examples.normal;

import org.apache.rocketmq.proxy.examples.common.entity.Msg;
import org.apache.rocketmq.proxy.examples.common.result.AbstractChecker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplyChecker extends AbstractChecker {
    public static ReplyChecker resultChecker = new ReplyChecker();
    private final Logger log = LoggerFactory.getLogger(ReplyChecker.class);

    public static ReplyChecker getInstance() {
        return resultChecker;
    }

    private ReplyChecker() {
    }

    @Override
    protected boolean check() {
        int nextIndex = next(ReplyTest.ID);
        log.info("reply next: {}", nextIndex);
        return nextIndex == ReplyTest.COUNT;
    }

    public void record(Msg msg) {
        if (!record(msg.getId(), msg.getIndex() - (1 << 16))) {
            log.error("reply record fail, msg: {}", msg);
        }
    }
}
