package org.qcri.rheem.core.types;

import java.util.Objects;

/**
 * A basic data unit type is elementary and not constructed from other data unit types.
 */
public class BasicDataUnitType<T> extends DataUnitType<T> {

    private final Class<T> typeClass;

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
