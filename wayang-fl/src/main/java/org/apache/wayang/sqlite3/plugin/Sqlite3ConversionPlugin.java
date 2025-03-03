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

package org.apache.wayang.sqlite3.plugin;

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.mapping.Mapping;
import org.apache.wayang.core.optimizer.channels.ChannelConversion;
import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.core.plugin.Plugin;
import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.wayang.sqlite3.channels.ChannelConversions;
import org.apache.wayang.sqlite3.platform.Sqlite3Platform;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * This {@link Plugin} provides {@link ChannelConversion}s from the  {@link Sqlite3Platform}.
 */
public class Sqlite3ConversionPlugin implements Plugin {

    @Override
    public Collection<Platform> getRequiredPlatforms() {
        return Arrays.asList(Sqlite3Platform.getInstance(), JavaPlatform.getInstance());
    }

    @Override
    public Collection<Mapping> getMappings() {
        return Collections.emptyList();
    }

    @Override
    public Collection<ChannelConversion> getChannelConversions() {
        return ChannelConversions.ALL;
    }

    @Override
    public void setProperties(Configuration configuration) {
    }
}
