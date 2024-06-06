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
package org.apache.rocketmq.proxy.examples.common.tools;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class Unirate {
    public static final long NANOS = 1000000;
    private volatile long rate;
    private final AtomicLong last = new AtomicLong(0);

    public Unirate(long qps) {
        rate = 1000 * NANOS / qps;
    }

    public void setQps(long qps) {
        rate = 1000 * NANOS / qps;
    }

    public long next() {
        long now = System.nanoTime();
        long prev = last.get();
        if (prev < now) {
            last.set(now + rate);
            return 0;
        }
        last.addAndGet(rate);
        return prev - now;
    }

    public void run(Consumer<Long> consumer) {
        long next = this.next();
        if (next > Unirate.NANOS / 2) {
            CountDownLatch c = new CountDownLatch(1);
            try {
                c.await(next, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        consumer.accept(next);
    }
}
