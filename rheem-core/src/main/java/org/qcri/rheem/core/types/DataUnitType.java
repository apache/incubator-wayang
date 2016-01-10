package org.qcri.rheem.core.types;

/**
 * The type of data types designate the kind of data that are being passed between operators.
 */
public abstract class DataUnitType {

    /**
     * Tells whether this type can be interpreted as the target type
     *
     * @param targetType the target type
     * @return whether the two are compatible
     */
    public boolean isForwardCompatibleTo(DataUnitType targetType) {
        // By default, we assume that the two data types need to equal.
        return this.equals(targetType);
    }

    /**
     * Tells whether this data unit type represents groups of data units.
     */
    public abstract boolean isGrouped();

    /**
     * Tells whether this is a normal data unit type.
     */
    public boolean isPlain() {
        return !isGrouped();
    }

}
