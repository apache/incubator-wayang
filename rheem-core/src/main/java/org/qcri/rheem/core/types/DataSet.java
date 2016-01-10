package org.qcri.rheem.core.types;

/**
 * A data set is an abstraction of the Rheem programming model. Although never directly materialized, a data set
 * keeps track of type and structure of data units being passed between operators.
 */
public abstract class DataSet {

    /**
     * Type of the data units within the data set.
     */
    private final DataUnitType dataUnitType;

    /**
     * Creates a flat data set that contains basic data units. This is the normal case.
     */
    public static FlatDataSet flatAndBasic(Class<?> dataUnitClass) {
        return new FlatDataSet(new BasicDataUnitType(dataUnitClass));
    }

    protected DataSet(DataUnitType dataUnitType) {
        this.dataUnitType = dataUnitType;
    }

    public DataUnitType getDataUnitType() {
        return dataUnitType;
    }

    public abstract boolean isCompatibleTo(DataSet otherDataSet);

    protected abstract boolean isCompatibleToGroupedDataSet(GroupedDataSet groupedDataSet);

    protected abstract boolean isCompatibleToFlatDataSet(FlatDataSet flatDataSet);

    @Override
    public String toString() {
        return String.format("%s[%s]", this.getClass(), this.dataUnitType);
    }
}
