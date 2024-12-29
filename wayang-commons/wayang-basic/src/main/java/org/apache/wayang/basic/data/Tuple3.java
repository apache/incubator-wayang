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

package org.apache.wayang.basic.data;

import java.io.Serializable;
import java.util.Objects;

/**
 * A type for tuples. Might be replaced by existing classes for this purpose, such as from the Scala library.
 */
public class Tuple3<T0, T1, T2> implements Serializable {

    public T0 field0;

    public T1 field1;

    public T2 field2;

    public Tuple3() {
    }

    public Tuple3(T0 field0, T1 field1, T2 field2) {
        this.field0 = field0;
        this.field1 = field1;
        this.field2 = field2;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || this.getClass() != o.getClass()) return false;
        Tuple3<?, ?, ?> tuple2 = (Tuple3<?, ?, ?>) o;
        return Objects.equals(this.field0, tuple2.field0) &&
                Objects.equals(this.field1, tuple2.field1) &&
                Objects.equals(this.field2, tuple2.field2);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.field0, this.field1, this.field2);
    }

    @Override
    public String toString() {
        return String.format("(%s, %s, %s)", this.field0, this.field1, this.field2);
    }

    public T0 getField0() {
        return this.field0;
    }

    public T1 getField1() {
        return this.field1;
    }

    public T2 getField2() {
        return this.field2;
    }

    /**
     * @return a new instance with the first two fields of this instance swapped
     */
    public Tuple3<T1, T0, T2> swap() {
        return new Tuple3<>(this.field1, this.field0, this.field2);
    }
}
