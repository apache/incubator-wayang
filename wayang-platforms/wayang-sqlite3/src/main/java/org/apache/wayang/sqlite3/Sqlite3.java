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

package org.apache.wayang.sqlite3;

import org.apache.wayang.sqlite3.platform.Sqlite3Platform;
import org.apache.wayang.sqlite3.plugin.Sqlite3ConversionPlugin;
import org.apache.wayang.sqlite3.plugin.Sqlite3Plugin;

/**
 * Register for relevant components of this module.
 */
public class Sqlite3 {

    private final static Sqlite3Plugin PLUGIN = new Sqlite3Plugin();

    private final static Sqlite3ConversionPlugin CONVERSION_PLUGIN = new Sqlite3ConversionPlugin();

    /**
     * Retrieve the {@link Sqlite3Plugin}.
     *
     * @return the {@link Sqlite3Plugin}
     */
    public static Sqlite3Plugin plugin() {
        return PLUGIN;
    }

    /**
     * Retrieve the {@link Sqlite3ConversionPlugin}.
     *
     * @return the {@link Sqlite3ConversionPlugin}
     */
    public static Sqlite3ConversionPlugin conversionPlugin() {
        return CONVERSION_PLUGIN;
    }


    /**
     * Retrieve the {@link Sqlite3Platform}.
     *
     * @return the {@link Sqlite3Platform}
     */
    public static Sqlite3Platform platform() {
        return Sqlite3Platform.getInstance();
    }

}
