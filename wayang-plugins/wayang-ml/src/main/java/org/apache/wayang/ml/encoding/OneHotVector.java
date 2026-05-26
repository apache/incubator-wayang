package org.apache.wayang.ml.encoding;

import java.util.HashMap;

import org.apache.wayang.core.api.exception.WayangException;

public class OneHotVector {
    public static final int TOPOLOGIES_LENGTH = 4;
    
    // Size of the encoding data for one operator
    public static final int OPERATOR_SIZE      = OneHotMappings.getPlatformsMapping().size() + 8;
    public static final int CONVERSION_SIZE    = OneHotMappings.getPlatformsMapping().size() + 3;
    public static final int OPERATORS_LENGTH   = OneHotMappings.getOperatorMapping().size() * OPERATOR_SIZE;
    public static final int CONVERSIONS_LENGTH = OneHotMappings.getOperatorMapping().size() * CONVERSION_SIZE;

    public static final int LENGTH = TOPOLOGIES_LENGTH + OPERATORS_LENGTH + CONVERSIONS_LENGTH + 1;

    private static int getPosition(final String operator) {
        final HashMap<String, Integer> operatorMapping = OneHotMappings.getOperatorMapping();
        if (!operatorMapping.containsKey(operator)) {
            return -1;
        }

        return operatorMapping.get(operator);
    }

    private final long[] entries = new long[OneHotVector.LENGTH];

    public OneHotVector(){
        System.out.println("initialized onehot vector with sizes:");
        System.out.println("op size: " + OPERATOR_SIZE);
        System.out.println("conversions size: " + CONVERSION_SIZE);
        System.out.println("op length: " + OPERATORS_LENGTH);
        System.out.println("conv length: " + CONVERSIONS_LENGTH);
    }

    public void addOperator(final long[] encodedOperator, final String operator) {
        assert encodedOperator.length == OPERATOR_SIZE : "amount of encoded operators was not equal to the operator size defined in one hot.";

        final int position = getPosition(operator);

        // position of operator couldnt be found
        if (position == -1) {
            throw new WayangException("Could not find position of operator, potentially illegal operator, got operator: " + operator);
        }

        for (int i = 0; i < encodedOperator.length; i++) {
            this.entries[TOPOLOGIES_LENGTH + i + (position * OPERATOR_SIZE)] = encodedOperator[i];
        }
    }

    public void addDataMovement(final long[] encodedConversion, final String operator) {
        assert encodedConversion.length == CONVERSIONS_LENGTH : "amount of encoded operators was not equal to the operator size defined in one hot.";
        final int position = getPosition(operator);

        // position of operator couldnt be found
        if (position == -1) {
            throw new WayangException("Could not find position of operator, potentially illegal operator, got operator: " + operator);
        }

        for (int i = 0; i < encodedConversion.length; i++) {
            this.entries[TOPOLOGIES_LENGTH + OPERATORS_LENGTH + i + (position * CONVERSION_SIZE)] = encodedConversion[i];
        }
    }

    public void setTopologies(final long[] topologies) {
        assert topologies.length == TOPOLOGIES_LENGTH : "amount of encoded operators was not equal to the operator size defined in one hot.";

        for (int i = 0; i < TOPOLOGIES_LENGTH; i++) {
            this.entries[i] = topologies[i];
        }
    }

    public long getDataset() {
        return this.entries[LENGTH - 1];
    }

    public void setDataset(final Long dataset) {
        this.entries[LENGTH - 1] = dataset;
    }

    public long[] getEntries() {
        return this.entries;
    }
}
