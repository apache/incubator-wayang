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

package org.apache.wayang.core.types;

import java.io.Serializable;
import java.util.Objects;

/**
 * A basic data unit type is elementary and not constructed from other data unit types.
 */
public class BasicDataUnitType<T> extends DataUnitType<T> implements Serializable {

    public BasicDataUnitType() {}

    private Class<T> typeClass;

    protected BasicDataUnitType(Class<T> typeClass) {
        this.typeClass = typeClass;
        // TODO: The class might have generics. In that case, this class does currently not fully describe the data unit type.
    }

    @Override
    public boolean isGroup() {
        return false;
    }

    @Override
    public BasicDataUnitType<T> toBasicDataUnitType() {
        return this;
    }

    /**
     * In generic code, we do not have the type parameter values of operators, functions etc. This method avoids casting issues.
     *
     * @return this instance with type parameters set to {@code T}
     */
    @SuppressWarnings("unchecked")
    public <T> DataUnitType<T> unchecked() {
        return (DataUnitType<T>) this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || this.getClass() != o.getClass()) return false;
        BasicDataUnitType that = (BasicDataUnitType) o;
        return Objects.equals(this.typeClass, that.typeClass);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.typeClass);
    }

    @Override
    public String toString() {
        return String.format("%s[%s]", this.getClass().getSimpleName(), this.typeClass.getSimpleName());
    }

    @Override
    public Class<T> getTypeClass() {
        return this.typeClass;
    }
}
