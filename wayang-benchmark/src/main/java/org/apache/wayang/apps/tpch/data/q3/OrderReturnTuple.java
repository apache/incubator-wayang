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

package org.apache.wayang.apps.tpch.data.q3;

import java.io.Serializable;

/**
 * Tuple that is returned by Query 1.
 */
public class OrderReturnTuple implements Serializable {

    public int O_ORDERKEY;

    public int O_CUSTKEY;

    public int O_ORDERDATE;

    /**
     * {@code decimal}
     */
    public int O_SHIPPRIORITY;


    public OrderReturnTuple() {
    }

    public OrderReturnTuple(int o_ORDERKEY,
            int o_CUSTKEY,
            int o_ORDERDATE,
            int o_SHIPPRIORITY) {
        this.O_ORDERKEY = o_ORDERKEY;
        this.O_CUSTKEY = o_CUSTKEY;
        this.O_ORDERDATE = o_ORDERDATE;
        this.O_SHIPPRIORITY = o_SHIPPRIORITY;
    }

    @Override
    public String toString() {
        return "OrderReturnTuple{" +
                "O_ORDERKEY=" + this.O_ORDERKEY +
                ", O_CUSTKEY" + this.O_CUSTKEY +
                ", O_ORDERDATE=" + this.O_ORDERDATE +
                ", O_SHIPPRIORITY=" + this.O_SHIPPRIORITY +
                '}';
    }
}
