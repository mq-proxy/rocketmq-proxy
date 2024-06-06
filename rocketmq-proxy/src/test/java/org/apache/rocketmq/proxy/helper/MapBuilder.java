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
package org.apache.rocketmq.proxy.helper;

import java.util.HashMap;

public class MapBuilder<K, V> {
    HashMap<K, V> map = new HashMap<>();

    public HashMap<K, V> build() {
        return this.map;
    }

    public MapBuilder<K, V> add(K k, V v) {
        this.map.put(k, v);
        return this;
    }

    public static <K, V> MapBuilder<K, V> builder(K k, V v) {
        return new MapBuilder<K, V>().add(k, v);
    }
}
