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

package org.apache.wayang.basic.operators;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.plan.wayangplan.UnarySource;
import org.apache.wayang.core.types.DataSetType;

/**
 * Logical operator representing a GeoJSON file source producing {@link Record} elements.
 */
public class GeoJsonFileSource extends UnarySource<Record> {

    private final String inputUrl;

    public GeoJsonFileSource(String inputUrl) {
        super(DataSetType.createDefault(Record.class));
        this.inputUrl = inputUrl;
    }

    public GeoJsonFileSource(GeoJsonFileSource that) {
        super(that);
        this.inputUrl = that.getInputUrl();
    }

    public String getInputUrl() {
        return inputUrl;
    }
}
