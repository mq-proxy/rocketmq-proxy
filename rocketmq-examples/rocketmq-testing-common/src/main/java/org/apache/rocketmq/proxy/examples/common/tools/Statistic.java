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

public class Statistic {
    private long startTime = System.currentTimeMillis();
    private long endTime = System.currentTimeMillis();

    private long total = 0;
    private long count = 0;
    private long min = Long.MAX_VALUE;
    private long max = 0;

    public synchronized void record(long num) {
        if (num < 0) {
            return;
        }
        total += num;
        count++;
        min = Math.min(min, num);
        max = Math.max(max, num);
        endTime = System.currentTimeMillis();
    }

    public static Statistic statistics(Statistic... statistics) {
        Statistic result = new Statistic();
        if (statistics == null) {
            return result;
        }
        for (Statistic statistic : statistics) {
            result.startTime = Math.min(statistic.startTime, result.startTime);
            result.endTime = Math.max(statistic.endTime, result.endTime);
            result.total = result.total + statistic.total;
            result.count = result.count + statistic.count;
            result.min = Math.min(statistic.min, result.min);
            result.max = Math.max(statistic.max, result.max);
        }
        return result;
    }

    public void reset() {
        this.total = 0;
        this.count = 0;
        this.min = Long.MAX_VALUE;
        this.max = 0;
        this.startTime = System.currentTimeMillis();
        this.endTime = System.currentTimeMillis();
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public long getTotal() {
        return total;
    }

    public long getCount() {
        return count;
    }

    public long getMin() {
        return min == Long.MAX_VALUE ? 0 : min;
    }

    public long getMax() {
        return max;
    }

    public double getAvg() {
        return total == 0 ? 0 : 1.0 * total / count;
    }

    @Override
    public String toString() {
        return String.format("Statistic{total: %d, count: %d, avg: %f, min: %d, max: %d}",
                getTotal(),
                getCount(),
                getAvg(),
                getMin(),
                getMax()
        );
    }
}
