/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.ml.encoding;

import java.util.HashMap;

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

        return !operatorMapping.containsKey(operator) ? -1 : operatorMapping.get(operator);
    }

    private final long[] entries = new long[OneHotVector.LENGTH];

    public OneHotVector(){
    }

    public void addOperator(final long[] encodedOperator, final String operator) {
        assert encodedOperator.length == OPERATOR_SIZE :
            "Invalid encoded operator size: expected " + OPERATOR_SIZE +
            " but got " + encodedOperator.length +
            " for operator [" + operator + "].";

        final int position = getPosition(operator);

        // position of operator couldnt be found
        if (position == -1) {
            return;
            //throw new WayangException("Could not find position of operator, potentially illegal operator, got operator: " + operator);
        }

        for (int i = 0; i < encodedOperator.length; i++) {
            this.entries[TOPOLOGIES_LENGTH + i + (position * OPERATOR_SIZE)] = encodedOperator[i];
        }
    }

    public void addDataMovement(final long[] encodedConversion, final String operator) {
        assert encodedConversion.length == CONVERSION_SIZE : "amount of encoded operators was not equal to the operator size defined in one hot. Got: " + encodedConversion.length + ", expected: " + CONVERSION_SIZE;
        final int position = getPosition(operator);

        // position of operator couldnt be found
        if (position == -1) {
            return;
            //throw new WayangException("Could not find position of operator, potentially illegal operator, got operator: " + operator);
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
