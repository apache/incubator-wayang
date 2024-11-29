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

import org.apache.wayang.core.util.Copyable;
import org.apache.wayang.core.util.ReflectionUtils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * A Type that represents a record with a schema, might be replaced with something standard like JPA entity.
 */
public class Record implements Serializable, Copyable<Record> {

    private Object[] values;

    public Record(Object... values) {
        this.values = values;
    }

    public Record(List<Object> values) {
        this.values = values.toArray();
    }

    @Override
    public Record copy() {
        return new Record(this.values.clone());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || this.getClass() != o.getClass()) return false;
        Record record2 = (Record) o;
        return Arrays.equals(this.values, record2.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(values));
    }

    @Override
    public String toString() {
        return "Record" + Arrays.toString(this.values);
    }

    public Object getField(int index) {
        return this.values[index];
    }

    /**
     * Retrieve a field as a {@code double}. It must be castable as such.
     *
     * @param index the index of the field
     * @return the {@code double} representation of the field
     */
    public double getDouble(int index) {
        Object field = this.values[index];
        return ReflectionUtils.toDouble(field);
       }

    /**
     * Retrieve a field as a {@code long}. It must be castable as such.
     *
     * @param index the index of the field
     * @return the {@code long} representation of the field
     */
    public long getLong(int index) {
        Object field = this.values[index];
        if (field instanceof Integer) return (Integer) field;
        else if (field instanceof Long) return (Long) field;
        else if (field instanceof Short) return (Short) field;
        else if (field instanceof Byte) return (Byte) field;
        throw new IllegalStateException(String.format("%s cannot be retrieved as long.", field));
    }

    /**
     * Retrieve a field as a {@code int}. It must be castable as such.
     *
     * @param index the index of the field
     * @return the {@code int} representation of the field
     */
    public int getInt(int index) {
        Object field = this.values[index];
        if (field instanceof Integer) return (Integer) field;
        else if (field instanceof Short) return (Short) field;
        else if (field instanceof Byte) return (Byte) field;
        throw new IllegalStateException(String.format("%s cannot be retrieved as int.", field));
    }

    /**
     * Retrieve a field as a {@link String}.
     *
     * @param index the index of the field
     * @return the field as a {@link String} (obtained via {@link Object#toString()}) or {@code null} if the field is {@code null}
     */
    public String getString(int index) {
        Object field = this.values[index];
        return field == null ? null : field.toString();
    }

    /**
     * Retrieve the size of this instance.
     *
     * @return the number of fields in this instance
     */
    public int size() {
        return this.values.length;
    }

}
