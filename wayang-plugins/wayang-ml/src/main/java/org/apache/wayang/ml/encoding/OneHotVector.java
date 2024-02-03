package org.apache.wayang.ml.encoding;

import org.apache.wayang.core.plan.wayangplan.OperatorBase;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.ml.encoding.OneHotMappings;

import org.reflections.*;

import java.util.HashMap;
import java.util.Vector;
import java.util.Set;
import java.util.stream.Collectors;

public class OneHotVector {
    private long[] entries;

    private Long dataset;

    public int length;

    // Size of the encoding data for one operator
    public static int OPERATOR_SIZE = 10;

    // Size of the encoding data for one conversion operator
    public static int CONVERSION_SIZE = 4;

    public static int TOPOLOGIES_LENGTH = 4;

    public static OneHotMappings oneHotMapping = OneHotMappings.getInstance();

    public static int operatorsLength =
        oneHotMapping.getOperatorMapping().size() * OPERATOR_SIZE;

    public static int conversionsLength =
        oneHotMapping.getConversionMapping().size() * CONVERSION_SIZE;

    public OneHotVector() {
        this.length = TOPOLOGIES_LENGTH + operatorsLength + conversionsLength + 1;
        this.entries = new long[this.length];
        System.out.println("ONE HOT VECTOR SIZE: " + this.length);
        System.out.println("ONE HOT OPERATOR SIZE: " + operatorsLength);
        System.out.println("ONE HOT DM SIZE: " + conversionsLength);
    }

    public void addOperator(long[] encodedOperator, String operator) {
        int position = getOperatorPosition(operator);

        //position of operator couldnt be found
        if (position == -1) {
            return;
        }

        for (int i = 0; i < encodedOperator.length; i++) {
            this.entries[4 + i + (position * OPERATOR_SIZE)] = encodedOperator[i];
        }
    }

    private static int getOperatorPosition(String operator) {
        HashMap<String, Integer> operatorMapping = oneHotMapping.getOperatorMapping();
        if (!operatorMapping.containsKey(operator)) {
            return -1;
        }

        return operatorMapping.get(operator);
    }

    public void addDataMovement(long[] encodedConversion, String operator) {
        int position = getConversionOperatorPosition(operator);

        //position of operator couldnt be found
        if (position == -1) {
            return;
        }

        for (int i = 0; i < encodedConversion.length; i++) {
            this.entries[4 + operatorsLength + i + (position * CONVERSION_SIZE)] = encodedConversion[i];
        }
    }

    private static int getConversionOperatorPosition(String operator) {
        HashMap<String, Integer> conversionMapping = oneHotMapping.getConversionMapping();
        if (!conversionMapping.containsKey(operator)) {
            return -1;
        }

        return conversionMapping.get(operator);
    }

    public void setTopologies(long[] topologies) {
        for (int i = 0; i < TOPOLOGIES_LENGTH; i++) {
            this.entries[i] = topologies[i];
        }
    }

    public long getDataset() {
        return this.entries[this.length - 1];
    }

    public void setDataset(Long dataset) {
        this.entries[this.length - 1] = dataset;
    }

    public long[] getEntries() {
        return this.entries;
    }

}
