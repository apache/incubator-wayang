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

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.function.FunctionDescriptor;

public class SortFilter implements FunctionDescriptor.SerializablePredicate<Record> {
    final int offset;
    final int fetch;
    int increment;

    /**
     * The filter for a calcite/sql sort operator
     * usually triggered by "LIMIT x", "OFFSET x", "FETCH x" statements
     * @param offset amount of records ignored before accepting
     * @param fetch amount of records accepted
     */
    public SortFilter(final int fetch, final int offset) {
        this.fetch = fetch;
        this.offset = offset;
    }

    @Override
    public boolean test(final Record record) {
        final boolean test = increment >= offset && increment <= fetch;
        increment++;

        return test;
    }
}