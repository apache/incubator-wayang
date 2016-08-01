package org.qcri.rheem.basic.types;


import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.core.types.BasicDataUnitType;

import java.util.Arrays;
import java.util.Objects;

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
