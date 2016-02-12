package org.qcri.rheem.core.types;

import java.util.Iterator;
import java.util.Objects;

/**
 * A data set is an abstraction of the Rheem programming model. Although never directly materialized, a data set
 * keeps track of type and structure of data units being passed between operators.
 */
public class DataSetType<T> {

    /**
     * Type of the data units within the data set.
     */
    private final DataUnitType<T> dataUnitType;

    /**
     * Creates a flat data set that contains basic data units. This is the normal case.
     */
    public static <T> DataSetType<T> createDefault(Class<? extends T> dataUnitClass) {
        return new DataSetType<>(new BasicDataUnitType<>(dataUnitClass));
    }

    /**
     * Creates a flat data set that contains basic data units. This is the normal case.
     */
    public static <T> DataSetType<T> createDefaultUnchecked(Class<?> dataUnitClass) {
        return new DataSetType<>(new BasicDataUnitType<>(dataUnitClass));
    }

    /**
     * Creates a data set that contains groups of data units.
     */
    public static <T> DataSetType<Iterator<T>> createGrouped(Class<? extends T> dataUnitClass) {
        return new DataSetType<>(new DataUnitGroupType<>(new BasicDataUnitType<>(dataUnitClass)));
    }
    /**
     *
     * Creates a data set that contains groups of data units.
     */
    public static <T> DataSetType<Iterator<T>> createGroupedUnchecked(Class<?> dataUnitClass) {
        return new DataSetType<>(new DataUnitGroupType<>(new BasicDataUnitType<>(dataUnitClass)));
    }

    protected DataSetType(DataUnitType dataUnitType) {
        this.dataUnitType = dataUnitType;
    }

    public DataUnitType<T> getDataUnitType() {
        return this.dataUnitType;
    }

    /**
     * In generic code, we do not have the type parameter values of operators, functions etc. This method avoids casting issues.
     *
     * @return this instance with type parameters set to {@link Object}
     */
    @SuppressWarnings("unchecked")
    public DataSetType<Object> unchecked() {
        return (DataSetType<Object>) this;
    }

    public boolean isCompatibleTo(DataSetType otherDataSetType) {
        return this.dataUnitType.equals(otherDataSetType.dataUnitType);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || this.getClass() != o.getClass()) return false;
        DataSetType<?> that = (DataSetType<?>) o;
        return Objects.equals(this.dataUnitType, that.dataUnitType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.dataUnitType);
    }

    @Override
    public String toString() {
        return String.format("%s[%s]", this.getClass().getSimpleName(), this.dataUnitType);
    }
}
