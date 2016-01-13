package org.qcri.rheem.core.types;

import java.util.Iterator;
import java.util.Objects;

/**
 * A grouped data unit type describes just the structure of data units within a grouped dataset.
 */
public class DataUnitGroupType<T> extends DataUnitType<Iterator<T>> {

    private final DataUnitType<T> baseType;

    protected DataUnitGroupType(DataUnitType baseType) {
        this.baseType = baseType;
    }

    @Override
    public boolean isGroup() {
        return true;
    }

    public DataUnitType getBaseType() {
        return baseType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataUnitGroupType that = (DataUnitGroupType) o;
        return Objects.equals(baseType, that.baseType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(baseType);
    }

    @Override
    public String toString() {
        return String.format("%s[%s]", this.getClass().getSimpleName(), this.baseType);
    }
}
