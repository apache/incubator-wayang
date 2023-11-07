/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.ml4all.utils;

import java.io.Serializable;
import java.util.Arrays;

public class SparseVector implements Serializable {
    double label; //optional
    int[] indices;
    double[] values;

    public SparseVector () {

    }

    public SparseVector(int[] indices, double[] values) {
        this.indices = indices;
        this.values = values;
    }

    public SparseVector(double label, int[] indices, double[] values) {
        this.label = label;
        this.indices = indices;
        this.values = values;
    }

    public int[] getIndices() {
        return indices;
    }

    public double[] getValues() {
        return values;
    }

    public double getLabel() { return label; }

    public double getDenseValue (int index) {
        for (int i = 0; i < indices.length; i++) {
            if (index == indices[i])
                return values[i];
        }
        return 0;
    }

    public void setLabel(double label) {
        this.label = label;
    }

    public void setIndices(int[] indices) {
        this.indices = indices;
    }

    public void setValues(double[] values) {
        this.values = values;
    }

    public int size() {
        return indices.length;
    }

    public SparseVector add (SparseVector vector) {
        int size = this.size() + vector.size();
        int[] sumInd = new int[size];
        double[] sumVal = new double[size];

        int index1 = 0;
        int index2 = 0;
        int i = 0;
        while (index1 < this.size() && index2 < vector.size()) {
            if (this.indices[index1] < vector.getIndices()[index2]) {
                sumInd[i] = this.indices[index1];
                sumVal[i] = this.values[index1];
                index1++;
            }
            else if (this.indices[index1] > vector.getIndices()[index2]) {
                sumInd[i] = vector.getIndices()[index2];
                sumVal[i] = vector.getValues()[index2];
                index2++;
            }
            else { //they are equal
                sumInd[i] = this.indices[index1];
                sumVal[i] = this.values[index1] + vector.getValues()[index2];
                index1++; index2++;
            }
            i++;
        }

        //add remaining elements for one of the two
        for (int j = index1; j < this.size(); j++) {
            sumInd[i] = this.indices[j];
            sumVal[i] = this.values[j];
            i++;
        }
        for (int j = index2; j < vector.size(); j++) {
            sumInd[i] = vector.getIndices()[j];
            sumVal[i] = vector.getValues()[j];
            i++;
        }

        return new SparseVector(this.getLabel() + vector.getLabel(), Arrays.copyOf(sumInd, i), Arrays.copyOf(sumVal, i));
    }

    public SparseVector add2 (SparseVector vector) {

        double fractionofSmallArray = 0.2;
        //take the largest vector and add a % of the other vector
        int estimatedSize = (vector.size() < this.size()) ? this.size() + (int) Math.ceil(fractionofSmallArray * vector.size())  : vector.size() + (int) Math.ceil(fractionofSmallArray * this.size());

        int[] currentInd = new int[estimatedSize];
        double[] currentVal = new double[estimatedSize];
        int[] previousInd = null;
        double[] previousVal = null;

        int index1 = 0;
        int index2 = 0;
        int i = 0;
        boolean overflow = false;
        while (index1 < this.size() && index2 < vector.size()) {
            if (this.indices[index1] < vector.getIndices()[index2]) {
                currentInd[i] = this.indices[index1];
                currentVal[i] = this.values[index1];
                index1++;
            }
            else if (this.indices[index1] > vector.getIndices()[index2]) {
                currentInd[i] = vector.getIndices()[index2];
                currentVal[i] = vector.getValues()[index2];
                index2++;
            }
            else { //they are equal
                currentInd[i] = this.indices[index1];
                currentVal[i] = this.values[index1] + vector.getValues()[index2];
                index1++; index2++;
            }
            i++;

            if (!overflow && i == estimatedSize) { //reached estimated size
                overflow = true;
                previousInd = currentInd;
                previousVal = currentVal;
                currentInd = new int[this.size() - index1 + vector.size() - index2];
                currentVal = new double[this.size() - index1 + vector.size() - index2];
                i = 0;
            }
        }

        for (int j = index1; j < this.size(); j++) {
            currentInd[i] = this.indices[j];
            currentVal[i] = this.values[j];
            i++;
            if (!overflow && i == estimatedSize) { //reached estimated size
                overflow = true;
                previousInd = currentInd;
                previousVal = currentVal;
                currentInd = new int[this.size() - index1 + vector.size() - index2];
                currentVal = new double[this.size() - index1 + vector.size() - index2];
                i = 0;
            }
        }
        for (int j = index2; j < vector.size(); j++) {
            currentInd[i] = vector.getIndices()[j];
            currentVal[i] = vector.getValues()[j];
            i++;
            if (!overflow && i == estimatedSize) { //reached estimated size
                overflow = true;
                previousInd = currentInd;
                previousVal = currentVal;
                currentInd = new int[this.size() - index1 + vector.size() - index2];
                currentVal = new double[this.size() - index1 + vector.size() - index2];
                i = 0;
            }
        }

        if (overflow)
            return new SparseVector(this.getLabel() + vector.getLabel(), concat(previousInd, Arrays.copyOf(currentInd,i)), concat(previousVal, Arrays.copyOf(currentVal, i)));
        else
            return new SparseVector(this.getLabel() + vector.getLabel(), Arrays.copyOf(currentInd, i), Arrays.copyOf(currentVal, i));
    }

    public boolean isEmpty() {
        return (indices.length > 0 ? false : true);
    }

    public double[] concat(double[] a, double[] b) {
        int aLen = a.length;
        int bLen = b.length;
        double[] c = new double[aLen+bLen];
        System.arraycopy(a, 0, c, 0, aLen);
        System.arraycopy(b, 0, c, aLen, bLen);
        return c;
    }

    public int[] concat(int[] a, int[] b) {
        int aLen = a.length;
        int bLen = b.length;
        int[] c = new int[aLen+bLen];
        System.arraycopy(a, 0, c, 0, aLen);
        System.arraycopy(b, 0, c, aLen, bLen);
        return c;
    }

    public String toString() {
        return + label + " {" + Arrays.toString(indices) + "}, {" + Arrays.toString(values) + " }";
    }
}
