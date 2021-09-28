/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.wayang.plugin.hackit.core.tuple.header;

import java.util.HashMap;
import java.util.Map;

/**
 * HeaderBuilder it the generator of {@link Header} to one kind of {@link org.apache.wayang.plugin.hackit.core.tuple.HackitTuple}
 */
public class HeaderBuilder {

    //TODO: Use Wayang Configuration
    Map<String, String> configuration;

    /**
     * Default Construct
     */
    public HeaderBuilder(){
        configuration = new HashMap<>();
        //TODO: take from the configuration
    }

    /**
     * generate a new Header depending on the configuration provided
     *
     * @param <T> is the type that will be provided
     * @return {@link Header} is new instance of the header requested
     */
    public <T> Header<T> generateHeader(){
        //TODO: take and works from the configuration provided either on a file or by parameters at runtime
        return (Header<T>) new HeaderLong(this.configuration);
    }

    public Map<String, String> getConfiguration() {
        return configuration;
    }

    public String getConfiguration(String key) {
        return configuration.get(key);
    }

    public void setConfiguration(Map<String, String> configuration) {
        this.configuration = configuration;
    }

    public void setConfiguration(String key, String value){
        this.configuration.put(key, value);
    }
}
