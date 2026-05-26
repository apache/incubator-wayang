package org.apache.wayang.ml.encoding;

import java.util.HashMap;

public class OneHotVector {
    public static final int TOPOLOGIES_LENGTH = 4;

    // Size of the encoding data for one operator
    public static final int OPERATOR_SIZE = OneHotMappings.getPlatformsMapping().size() + 8;
    public static final int CONVERSION_SIZE = OneHotMappings.getPlatformsMapping().size() + 3;
    public static final int operatorsLength = OneHotMappings.getOperatorMapping().size() * OPERATOR_SIZE;
    public static final int conversionsLength = OneHotMappings.getOperatorMapping().size() * CONVERSION_SIZE;

    private static int getOperatorPosition(final String operator) {
        final HashMap<String, Integer> operatorMapping = OneHotMappings.getOperatorMapping();
        if (!operatorMapping.containsKey(operator)) {
            return -1;
        }

        return operatorMapping.get(operator);
    }

    private static int getConversionOperatorPosition(final String operator) {
        final HashMap<String, Integer> conversionMapping = OneHotMappings.getOperatorMapping();
        if (!conversionMapping.containsKey(operator)) {
            return -1;
        }

        return conversionMapping.get(operator);
    }

    private static final long[] entries = new long[OneHotVector.length];

    public static final int length = TOPOLOGIES_LENGTH + operatorsLength + conversionsLength + 1;

    public void addOperator(final long[] encodedOperator, final String operator) {
        final int position = getOperatorPosition(operator);

        // position of operator couldnt be found
        if (position == -1) {
            return;
        }

        for (int i = 0; i < encodedOperator.length; i++) {
            OneHotVector.entries[TOPOLOGIES_LENGTH + i + (position * OPERATOR_SIZE)] = encodedOperator[i];
        }
    }

    public void addDataMovement(final long[] encodedConversion, final String operator) {
        final int position = getConversionOperatorPosition(operator);

        // position of operator couldnt be found
        if (position == -1) {
            return;
        }

        for (int i = 0; i < encodedConversion.length; i++) {
            OneHotVector.entries[TOPOLOGIES_LENGTH + operatorsLength + i + (position * CONVERSION_SIZE)] = encodedConversion[i];
        }
    }

    public void setTopologies(final long[] topologies) {
        for (int i = 0; i < TOPOLOGIES_LENGTH; i++) {
            OneHotVector.entries[i] = topologies[i];
        }
    }

    public long getDataset() {
        return OneHotVector.entries[OneHotVector.length - 1];
    }

    public void setDataset(final Long dataset) {
        OneHotVector.entries[OneHotVector.length - 1] = dataset;
    }

    public long[] getEntries() {
        return OneHotVector.entries;
    }
}
