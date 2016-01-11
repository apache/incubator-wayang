package org.qcri.rheem.core.types;

import java.util.Objects;

/**
 * A basic data unit type is elementary and not constructed from other data unit types.
 */
public class BasicDataUnitType extends DataUnitType {

    private final Class<?> typeClass;

    public BasicDataUnitType(Class<?> typeClass) {
        this.typeClass = typeClass;
    }

    @Override
    public boolean isGroup() {
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BasicDataUnitType that = (BasicDataUnitType) o;
        return Objects.equals(typeClass, that.typeClass);
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeClass);
    }

    @Override
    public String toString() {
        return String.format("%s[%s]", this.getClass().getSimpleName(), this.typeClass.getSimpleName());
    }
}
