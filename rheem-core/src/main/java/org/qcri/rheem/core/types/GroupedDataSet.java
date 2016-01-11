package org.qcri.rheem.core.types;

/**
 * A grouped data set contains groups of data units.
 */
public class GroupedDataSet extends DataSet {

    public GroupedDataSet(DataUnitType dataUnitType) {
        super(dataUnitType);
    }

    @Override
    public boolean isCompatibleTo(DataSet otherDataSet) {
        return otherDataSet.isCompatibleToGroupedDataSet(this);
    }

    @Override
    protected boolean isCompatibleToGroupedDataSet(GroupedDataSet that) {
        return this.getDataUnitType().equals(that.getDataUnitType());
    }

    @Override
    protected boolean isCompatibleToFlatDataSet(FlatDataSet flatDataSet) {
        // Play the ball back to FlatDataSet, it has the appropriate implementation.
        return flatDataSet.isCompatibleToGroupedDataSet(this);
    }
}
