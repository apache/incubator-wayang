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
import org.apache.wayang.core.plan.wayangplan.UnarySink;
import org.apache.wayang.core.types.DataSetType;

/**
 * Logical operator that writes {@link Record}s into a Parquet file.
 */
public class ParquetSink extends UnarySink<Record> {

    private final String outputUrl;

    private final boolean isOverwrite;

    private final boolean preferDataset;

    public ParquetSink(String outputUrl, boolean isOverwrite, boolean preferDataset, DataSetType<Record> type) {
        super(type);
        this.outputUrl = outputUrl;
        this.isOverwrite = isOverwrite;
        this.preferDataset = preferDataset;
    }

    public ParquetSink(String outputUrl, boolean isOverwrite, boolean preferDataset) {
        this(outputUrl, isOverwrite, preferDataset, DataSetType.createDefault(Record.class));
    }

    public String getOutputUrl() {
        return this.outputUrl;
    }

    public boolean isOverwrite() {
        return this.isOverwrite;
    }

    public boolean prefersDataset() {
        return this.preferDataset;
    }
}
