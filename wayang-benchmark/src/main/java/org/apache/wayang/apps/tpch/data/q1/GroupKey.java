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

package org.apache.wayang.apps.tpch.data.q1;

import java.io.Serializable;
import java.util.Objects;

/**
 * Grouping key used in Query 1.
 */
public class GroupKey implements Serializable {

    public char L_RETURNFLAG;

    public char L_LINESTATUS;

    public GroupKey(char l_RETURNFLAG, char l_LINESTATUS) {
        this.L_RETURNFLAG = l_RETURNFLAG;
        this.L_LINESTATUS = l_LINESTATUS;
    }

    public GroupKey() {
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || this.getClass() != o.getClass()) return false;
        GroupKey groupKey = (GroupKey) o;
        return this.L_RETURNFLAG == groupKey.L_RETURNFLAG &&
                this.L_LINESTATUS == groupKey.L_LINESTATUS;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.L_RETURNFLAG, this.L_LINESTATUS);
    }
}
