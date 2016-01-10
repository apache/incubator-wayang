package org.qcri.rheem.core.types;

/**
 * A flat data set is basically a bag of data units.
 */
public class FlatDataSet extends DataSet {

    public FlatDataSet(DataUnitType dataUnitType) {
        super(dataUnitType);
    }

    @Override
    public boolean isCompatibleTo(DataSet otherDataSet) {
        return otherDataSet.isCompatibleToFlatDataSet(this);
    }

    @Override
    protected boolean isCompatibleToGroupedDataSet(GroupedDataSet groupedDataSet) {
        // A flat data set can be compatible to grouped one, if it contains grouped data units.
        if (this.getDataUnitType().isGrouped()) {
            GroupedDataUnitType groupedDataUnitType = (GroupedDataUnitType) this.getDataUnitType();
            return groupedDataUnitType.getBaseType().equals(groupedDataSet.getDataUnitType());
        }
        return false;
    }

    @Override
    protected boolean isCompatibleToFlatDataSet(FlatDataSet that) {
        return this.getDataUnitType().equals(that.getDataUnitType());
    }

}
