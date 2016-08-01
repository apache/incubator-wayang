package org.qcri.rheem.core.types;

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
