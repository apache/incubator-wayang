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

package org.apache.wayang.basic.types;

import java.util.Arrays;
import java.util.Objects;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.types.BasicDataUnitType;

/**
 * This is a specific {@link BasicDataUnitType} for {@link Record}s. In particular, it adds schema information.
 */
public class RecordType extends BasicDataUnitType<Record> {

    /**
     * Names of fields in the described {@link Record}s in order of appearance.
     */
    private String[] fieldNames;

    /**
     * Creates a new instance.
     *
     * @param fieldNames names of fields in the described {@link Record}s in order of appearance
     */
    public RecordType(String... fieldNames) {
        super(Record.class);
        this.fieldNames = fieldNames;
    }

    public String[] getFieldNames() {
        return this.fieldNames;
    }

    @Override
    public boolean isSupertypeOf(BasicDataUnitType<?> that) {
        // A RecordType cannot have subtypes.
        return this.equals(that);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        RecordType that = (RecordType) o;
        return Arrays.equals(fieldNames, that.fieldNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), fieldNames);
    }

    @Override
    public String toString() {
        return "RecordType" + Arrays.toString(fieldNames);
    }

    /**
     * Returns the index of a field according to this instance.
     *
     * @param fieldName the name of the field
     * @return the index of the field
     */
    public int getIndex(String fieldName) {
        for (int i = 0; i < this.fieldNames.length; i++) {
            String name = this.fieldNames[i];
            if (name.equals(fieldName)) {
                return i;
            }
        }
        throw new IllegalArgumentException(String.format("No such field \"%s\" in %s.", fieldName, this));
    }
}
