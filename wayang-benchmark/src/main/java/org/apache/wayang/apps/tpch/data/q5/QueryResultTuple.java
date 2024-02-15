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

package org.apache.wayang.apps.tpch.data.q5;

import java.io.Serializable;

/**
 * Tuple that is returned by Query 3.
 */
public class QueryResultTuple implements Serializable {

    /**
     * {@code integer}, {@code PK}
     */
    public String N_NAME;

    /**
     * {@code integer}, {@code FK}
     */
    public double REVENUE;

    public QueryResultTuple() {
    }

    public QueryResultTuple(String n_NAME, double o_REVENUE) {
        this.N_NAME = n_NAME;
        this.REVENUE = o_REVENUE;
    }

    @Override
    public String toString() {
        return "QueryResultTuple{" +
                "N_NAME=" + this.N_NAME +
                ", REVENUE=" + this.REVENUE +
                '}';
    }
}
