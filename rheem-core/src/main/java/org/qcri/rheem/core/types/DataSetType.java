package org.qcri.rheem.core.types;

import org.qcri.rheem.core.util.ReflectionUtils;

import java.util.Objects;

/**
 * A data set is an abstraction of the Rheem programming model. Although never directly materialized, a data set
 * keeps track of type and structure of data units being passed between operators.
 */
public class DataSetType<T> {

    /**
     * Stands for the absence of an actual instance.
     */
    public static final DataSetType<Void> NONE = DataSetType.createDefault(Void.class);

    /**
     * Stands for the absence of an actual instance.
     */
    public static final DataSetType<Iterable<Void>> GROUPED_NONE = DataSetType.createGrouped(Void.class);

    /**
     * Type of the data units within the data set.
     */
    private final DataUnitType<T> dataUnitType;

    /**
     * Creates a flat data set that contains basic data units. This is the normal case.
     */
    public static <T> DataSetType<T> createDefault(Class<? extends T> dataUnitClass) {
        return new DataSetType<>(new BasicDataUnitType<>(ReflectionUtils.generalize(dataUnitClass)));
    }

    /**
     * Creates a flat data set that contains basic data units. This is the normal case.
     */
    public static <T> DataSetType<T> createDefault(BasicDataUnitType<T> dataUnitType) {
        return new DataSetType<>(dataUnitType);
    }

    /**
     * Creates a flat data set that contains basic data units. This is the normal case.
     */
    public static <T> DataSetType<T> createDefaultUnchecked(Class<?> dataUnitClass) {
        return new DataSetType<>(new BasicDataUnitType<>(dataUnitClass).unchecked());
    }

    /**
     * Creates a data set that contains groups of data units.
     */
    public static <T> DataSetType<Iterable<T>> createGrouped(Class<? extends T> dataUnitClass) {
        return new DataSetType<>(new DataUnitGroupType<>(new BasicDataUnitType<>(dataUnitClass)));
    }

    /**
     * Creates a new data set type that contains groups of data units.
     */
    public static <T> DataSetType<Iterable<T>> createGrouped(BasicDataUnitType<T> inputType) {
        return new DataSetType<>(new DataUnitGroupType<>(inputType));
    }

    /**
     * Creates a data set that contains groups of data units.
     */
    public static <T> DataSetType<Iterable<T>> createGroupedUnchecked(Class<?> dataUnitClass) {
        return new DataSetType<>(new DataUnitGroupType<>(new BasicDataUnitType<>(dataUnitClass)));
    }

    /**
     * Returns a null type.
     */
    public static DataSetType<Void> none() {
        return NONE;
    }

    /**
     * Returns a grouped null type.
     */
    public static DataSetType<Iterable<Void>> groupedNone() {
        return GROUPED_NONE;
    }


    protected DataSetType(DataUnitType<T> dataUnitType) {
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

    /**
     * In generic code, we do not have the type parameter values of operators, functions etc. This method avoids casting issues.
     *
     * @return this instance with type parameters set to {@link Iterable}{@code <}{@link Object}{@code >}
     */
    @SuppressWarnings("unchecked")
    public DataSetType<Iterable<Object>> uncheckedGroup() {
        return (DataSetType<Iterable<Object>>) this;
    }

    /**
     * Checks whether this instance is more general that the given one, i.e., it is a valid type of datasets that are
     * also valid under the given type.
     *
     * @param that the other instance
     * @return whether this instance is a super type of {@code that} instance
     */
    public boolean isSupertypeOf(DataSetType that) {
        return this.dataUnitType.toBasicDataUnitType().isSupertypeOf(that.dataUnitType.toBasicDataUnitType());
    }

    /**
     * Find out whether this corresponds to either {@link #none()} or {@link #groupedNone()}.
     *
     * @return whether above condition holds
     */
    public boolean isNone() {
        return this.equals(none()) || this.equals(groupedNone());
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
