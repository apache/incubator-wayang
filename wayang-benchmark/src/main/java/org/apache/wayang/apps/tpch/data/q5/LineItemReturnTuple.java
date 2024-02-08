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
 * Tuple that is returned by Query 5.
 */
public class LineItemReturnTuple implements Serializable {

    /**
     * {@code identifier}, {@code PK}
     */
    public long L_ORDERKEY;

    /**
     * {@code identifier}, {@code PK}
     */
    public long L_SUPPKEY;

    /**
     * {@code decimal}
     */
    public double L_EXTENDEDPRICE;


    public LineItemReturnTuple() {
    }

    public LineItemReturnTuple(
        long l_ORDERKEY,
        long l_SUPPKEY,
        double l_EXTENDEDPRICE) {
        this.L_ORDERKEY = l_ORDERKEY;
        this.L_SUPPKEY = l_SUPPKEY;
        this.L_EXTENDEDPRICE = l_EXTENDEDPRICE;
    }

    @Override
    public String toString() {
        return "LineItemReturnTuple{" +
                "L_ORDERKEY=" + this.L_ORDERKEY +
                ", L_SUPPKEY=" + this.L_SUPPKEY +
                ", L_EXTENDEDPRICE=" + this.L_EXTENDEDPRICE +
                '}';
    }
}
