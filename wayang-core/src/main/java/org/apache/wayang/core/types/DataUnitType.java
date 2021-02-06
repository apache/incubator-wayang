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

/**
 * The type of data types designate the kind of data that are being passed between operators.
 */
public abstract class DataUnitType<T> {

    /**
     * Tells whether this data unit type represents groups of data units.
     */
    public abstract boolean isGroup();

    /**
     * Tells whether this is a normal data unit type.
     */
    public boolean isPlain() {
        return !this.isGroup();
    }

    public static <T> DataUnitGroupType<T> createGrouped(Class<T> cls) {
        return createGroupedUnchecked(cls);
    }

    public static <T> BasicDataUnitType<T> createBasic(Class<T> cls) {
        return createBasicUnchecked(cls);
    }

    public static <T> DataUnitGroupType<T> createGroupedUnchecked(Class<?> cls) {
        return new DataUnitGroupType<>(createBasicUnchecked(cls));
    }

    @SuppressWarnings("unchecked")
    public static <T> BasicDataUnitType<T> createBasicUnchecked(Class<?> cls) {
        return new BasicDataUnitType<>((Class<T>) cls);
    }

    /**
     * Converts this instance into a {@link BasicDataUnitType}.
     *
     * @return the {@link BasicDataUnitType}
     */
    public abstract BasicDataUnitType<T> toBasicDataUnitType();

    /**
     * Checks whether the given instance is the same as this instance or more specific.
     *
     * @param that the other instance
     * @return whether this instance is a super type of {@code that} instance
     */
    public boolean isSupertypeOf(BasicDataUnitType<?> that) {
        return this.getTypeClass().isAssignableFrom(that.getTypeClass());
    }

    public abstract Class<T> getTypeClass();
}
