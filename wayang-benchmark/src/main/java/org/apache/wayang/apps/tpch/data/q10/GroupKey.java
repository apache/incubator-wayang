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
import java.util.Objects;

/**
 * Grouping key used in Query 10.
 */
public class GroupKey implements Serializable {

    public int C_CUSTKEY;

    public String C_NAME;

    /**
     * {@code decimal}
     */
    public double C_ACCTBAL;

    public String C_PHONE;

    public String N_NAME;

    /**
     * {@code string}
     */
    public String C_ADDRESS;

    /**
     * {@code string}
     */
    public String C_COMMENT;

    public GroupKey(
        int c_CUSTKEY,
        String c_NAME,
        double c_ACCTBAL,
        String c_PHONE,
        String n_NAME,
        String c_ADDRESS,
        String c_COMMENT
    ) {
        this.C_CUSTKEY = c_CUSTKEY;
        this.C_NAME = c_NAME;
        this.C_ACCTBAL = c_ACCTBAL;
        this.C_PHONE = c_PHONE;
        this.N_NAME = n_NAME;
        this.C_ADDRESS = c_ADDRESS;
        this.C_COMMENT = c_COMMENT;
    }

    public GroupKey() {
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || this.getClass() != o.getClass()) return false;
        GroupKey groupKey = (GroupKey) o;
        return this.C_CUSTKEY == groupKey.C_CUSTKEY &&
                this.C_NAME == groupKey.C_NAME &&
                this.N_NAME == groupKey.N_NAME &&
                this.C_PHONE == groupKey.C_PHONE &&
                this.C_ADDRESS == groupKey.C_ADDRESS &&
                this.C_COMMENT == groupKey.C_COMMENT &&
                this.C_ACCTBAL == groupKey.C_ACCTBAL;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            this.C_CUSTKEY,
            this.C_NAME,
            this.C_ACCTBAL,
            this.C_PHONE,
            this.N_NAME,
            this.C_ADDRESS,
            this.C_COMMENT
        );
    }
}
