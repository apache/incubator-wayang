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

package org.apache.wayang.apps.tpch.data.q10;

import java.io.Serializable;

/**
 * Tuple that is returned by Query 10.
 */
public class QueryResultTuple implements Serializable {

    public int C_CUSTKEY;

    public String C_NAME;

    public double REVENUE;

    /**
     * {@code decimal}
     */
    public double C_ACCTBAL;

    public String N_NAME;

    /**
     * {@code string}
     */
    public String C_ADDRESS;

    /**
     * {@code string}
     */
    public String C_PHONE;

    /**
     * {@code string}
     */
    public String C_COMMENT;

    public QueryResultTuple() {
    }

    public QueryResultTuple(
        int c_CUSTKEY,
        String c_NAME,
        double REVENUE,
        double c_ACCTBAL,
        String n_NAME,
        String c_ADDRESS,
        String c_PHONE,
        String c_COMMENT
    ) {
        this.C_CUSTKEY = c_CUSTKEY;
        this.C_NAME = c_NAME;
        this.REVENUE = REVENUE;
        this.C_ACCTBAL = c_ACCTBAL;
        this.N_NAME = n_NAME;
        this.C_ADDRESS = c_ADDRESS;
        this.C_PHONE = c_PHONE;
        this.C_COMMENT = c_COMMENT;
    }

    @Override
    public String toString() {
        return "QueryResultTuple{" +
                "C_CUSTKEY=" + this.C_CUSTKEY +
                ", C_NAME=" + this.C_NAME +
                ", REVENUE=" + this.REVENUE +
                ", C_ACCTBAL=" + this.C_ACCTBAL +
                ", N_NAME=" + this.N_NAME +
                ", C_ADDRESS=" + this.C_ADDRESS +
                ", C_PHONE=" + this.C_PHONE +
                ", C_COMMENT=" + this.C_COMMENT +
                '}';
    }
}
