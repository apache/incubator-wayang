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
public class Tuple5<T0, T1, T2, T3, T4> implements Serializable {

    public T0 field0;

    public T1 field1;

    public T2 field2;

    public T3 field3;

    public T4 field4;

    public Tuple5() {
    }

    public Tuple5(T0 field0, T1 field1, T2 field2, T3 field3, T4 field4) {
        this.field0 = field0;
        this.field1 = field1;
        this.field2 = field2;
        this.field3 = field3;
        this.field4 = field4;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || this.getClass() != o.getClass()) return false;
        Tuple5<?, ?, ?, ?, ?> tuple2 = (Tuple5<?, ?, ?, ?, ?>) o;
        return Objects.equals(this.field0, tuple2.field0) &&
                Objects.equals(this.field1, tuple2.field1) &&
                Objects.equals(this.field2, tuple2.field2) &&
                Objects.equals(this.field3, tuple2.field3) &&
                Objects.equals(this.field4, tuple2.field4);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.field0, this.field1, this.field2, this.field3, this.field4);
    }

    @Override
    public String toString() {
        return String.format("(%s, %s, %s, %s, %s)", this.field0, this.field1, this.field2, this.field3, this.field4);
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

    public T3 getField3() {
        return this.field3;
    }

    public T4 getField4() {
        return this.field4;
    }

    /**
     * @return a new instance with the fields of this instance swapped
     */
    public Tuple5<T4, T3, T2, T1, T0> swap() {
        return new Tuple5<>(this.field4, this.field3, this.field2, this.field1, this.field0);
    }
}
