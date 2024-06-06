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

import java.util.function.Function;

public class EnvTool {
    public static String envWithDefault(String propertyName, String name, String def) {
        return envWithDefault(propertyName, name, def, String::new);
    }

    public static int envWithDefault(String propertyName, String name, int def) {
        return envWithDefault(propertyName, name, def, Integer::parseInt);
    }

    public static boolean envWithDefault(String propertyName, String name, boolean def) {
        return envWithDefault(propertyName, name, def, Boolean::parseBoolean);
    }

    public static <T> T envWithDefault(String propertyName, String name, T def, Function<String, T> func) {
        T result = def;
        try {
            String envValue = System.getProperty(propertyName, System.getenv(name));
            result = envValue == null || envValue.isEmpty() ? def : func.apply(envValue);
        } catch (Throwable ignore) { // number format exception

        }
        return result;
    }
}
