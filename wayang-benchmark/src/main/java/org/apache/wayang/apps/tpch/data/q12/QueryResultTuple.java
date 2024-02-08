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

package org.apache.wayang.apps.tpch.data.q12;

import java.io.Serializable;

/**
 * Tuple that is returned by Query 12.
 */
public class QueryResultTuple implements Serializable {

    /**
     * {@code fixed text, size 10}
     */
    public String L_SHIPMODE;

    public int HIGH_LINE_COUNT;

    public int LOW_LINE_COUNT;

    public QueryResultTuple() {
    }

    public QueryResultTuple(String l_SHIPMODE, int HIGH_LINE_COUNT, int LOW_LINE_COUNT) {
        this.L_SHIPMODE = l_SHIPMODE;
        this.HIGH_LINE_COUNT = HIGH_LINE_COUNT;
        this.LOW_LINE_COUNT = LOW_LINE_COUNT;
    }

    @Override
    public String toString() {
        return "QueryResultTuple{" +
                "L_SHIPMODE=" + this.L_SHIPMODE +
                ", HIGH_LINE_COUNT=" + this.HIGH_LINE_COUNT +
                ", LOW_LINE_COUNT=" + this.LOW_LINE_COUNT +
                '}';
    }
}
