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

package org.apache.wayang.core.util;

import java.util.Objects;

/**
 * A helper data structure to manage two values without creating new classes.
 */
public class Tuple<T0, T1> {

    public T0 field0;

    public T1 field1;

    public Tuple() { }

    public Tuple(T0 field0, T1 field1) {
        this.field0 = field0;
        this.field1 = field1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || this.getClass() != o.getClass()) return false;
        Tuple<?, ?> tuple = (Tuple<?, ?>) o;
        return Objects.equals(this.field0, tuple.field0) &&
                Objects.equals(this.field1, tuple.field1);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.field0, this.field1);
    }

    @Override
    public String toString() {
        return String.format("(%s, %s)", this.field0, this.field1);
    }

    public T0 getField0() {
        return this.field0;
    }

    public void setField0(T0 field0) {
        this.field0 = field0;
    }

    public T1 getField1() {
        return this.field1;
    }

    public void setField1(T1 field1) {
        this.field1 = field1;
    }
}
