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

import java.util.Objects;
import org.apache.wayang.core.util.ReflectionUtils;

/**
 * A grouped data unit type describes just the structure of data units within a grouped dataset.
 */
public class DataUnitGroupType<T> extends DataUnitType<Iterable<T>> {

    private final DataUnitType<T> baseType;

    protected DataUnitGroupType(DataUnitType baseType) {
        this.baseType = baseType;
    }

    @Override
    public boolean isGroup() {
        return true;
    }

    @Override
    public Class<Iterable<T>> getTypeClass() {
        return ReflectionUtils.specify(Iterable.class);
    }

    public DataUnitType<T> getBaseType() {
        return this.baseType;
    }

    @Override
    public BasicDataUnitType<Iterable<T>> toBasicDataUnitType() {
        return createBasicUnchecked(Iterable.class);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || this.getClass() != o.getClass()) return false;
        DataUnitGroupType that = (DataUnitGroupType) o;
        return Objects.equals(this.baseType, that.baseType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.baseType);
    }

    @Override
    public String toString() {
        return String.format("%s[%s]", this.getClass().getSimpleName(), this.baseType);
    }
}
