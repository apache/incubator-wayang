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

package org.apache.wayang.ml.util;

import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.jdbc.platform.JdbcPlatformTemplate;
import org.apache.wayang.sqlite3.platform.Sqlite3Platform;
import org.apache.wayang.giraph.platform.GiraphPlatform;
import org.apache.wayang.genericjdbc.platform.GenericJdbcPlatform;
import org.apache.wayang.tensorflow.platform.TensorflowPlatform;
import org.reflections.Reflections;

import java.util.Set;
import java.util.HashSet;

public class Platforms {
    public static Set<Class<? extends Platform>> getPlatforms() {
        final Reflections reflections = new Reflections("org.apache.wayang");
        final Set<Class<? extends Platform>> platforms = reflections.getSubTypesOf(Platform.class);

        final Set<Class<? extends Platform>> disallowedPlatforms = new HashSet<>();
        disallowedPlatforms.add(JdbcPlatformTemplate.class);
        disallowedPlatforms.add(Sqlite3Platform.class);
        disallowedPlatforms.add(GiraphPlatform.class);
        disallowedPlatforms.add(GenericJdbcPlatform.class);
        disallowedPlatforms.add(TensorflowPlatform.class);

        platforms.removeAll(disallowedPlatforms);

        return platforms;
    }

    public static String getNamespace(final String platformName) {
        final String[] exploded = platformName.split("\\.");
        final StringBuilder strBuilder = new StringBuilder();
        for (int i = 0; i < 4; i++) {
            strBuilder.append(exploded[i]);
            if (i != 3)  {
                strBuilder.append(".");
            }
        }

        return strBuilder.toString();
    }
}

