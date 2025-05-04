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

package org.apache.wayang.api.sql.calcite.converter.functions;

import java.util.Arrays;
import java.util.function.Function;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.function.FunctionDescriptor;

public class MultiConditionJoinKeyExtractor implements FunctionDescriptor.SerializableFunction<Record, Record> {
        private final Integer[] indexes;

    /**
     * Extracts a key for a {@link WayangMultiConditionJoinVisitor}.
     * is a subtype of {@link Function}, {@link Serializable} (as required by engines which use serialisation i.e. flink/spark)
     * Takes an input {@link Record} & {@link Integer} key and maps it to a generic field object T.
     * Performs an unchecked cast when applied.
     * @param index key
     */
    public MultiConditionJoinKeyExtractor(final Integer... indexes) {
        this.indexes = indexes;
    }

    public Record apply(final Record record) {
        return new Record(Arrays.stream(indexes).map(record::getField).toArray());
    }
}
