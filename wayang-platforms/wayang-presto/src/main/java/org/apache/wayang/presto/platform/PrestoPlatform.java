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

package org.apache.wayang.presto.platform;

import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.jdbc.platform.JdbcPlatformTemplate;

/**
 * {@link Platform} implementation for Presto (PrestoDB).
 *
 * <p>The {@code configName} {@code "presto"} makes Wayang resolve every property
 * with the {@code wayang.presto.*} prefix — connection ({@code .jdbc.url},
 * {@code .jdbc.user}, {@code .jdbc.password}), the cost model, and the hardware
 * profile — with defaults loaded from {@code wayang-presto-defaults.properties}.
 */
public class PrestoPlatform extends JdbcPlatformTemplate {

    private static final String PLATFORM_NAME = "Presto";

    private static final String CONFIG_NAME = "presto";

    private static PrestoPlatform instance = null;

    public static PrestoPlatform getInstance() {
        if (instance == null) {
            instance = new PrestoPlatform();
        }
        return instance;
    }

    protected PrestoPlatform() {
        super(PLATFORM_NAME, CONFIG_NAME);
    }

    @Override
    public String getJdbcDriverClassName() {
        return "com.facebook.presto.jdbc.PrestoDriver";
    }

}
