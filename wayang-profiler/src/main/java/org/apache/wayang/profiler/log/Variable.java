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

package org.apache.wayang.profiler.log;

import java.util.Random;

/**
 * A variable that can be altered by an optimization algorithm.
 */
public class Variable {

    private final String id;

    private final int index;

    private double minValue = 0d, maxValue = Double.POSITIVE_INFINITY;

    public Variable(int index, String id) {
        this.index = index;
        this.id = id;
    }

    public String getId() {
        return this.id;
    }

    public double getValue(Individual individual) {
        return individual.getGenome()[this.index];
    }

    public double createRandomValue(Random random) {
        if (!Double.isFinite(this.minValue)) {
            if (!Double.isInfinite(this.maxValue))
                return random.nextGaussian() * 1e9;
            else
                return this.maxValue - Math.abs(random.nextGaussian()) * 1e9;
        } else if (!Double.isFinite(this.maxValue)) {
            return this.minValue + Math.abs(random.nextGaussian()) * 1e9;
        }
        return random.nextDouble() * (this.maxValue - this.minValue) + this.minValue;
    }

    public double mutate(double currentValue, Random random) {
        final double deltaSmoothing = 1E-9;
        if (!Double.isFinite(this.minValue)) {
            if (!Double.isInfinite(this.maxValue)) {
                return currentValue + random.nextGaussian() * Math.abs(currentValue);
            } else {
                double delta = this.maxValue - currentValue;
                return this.maxValue - Math.abs(random.nextGaussian()) * delta;
            }
        } else if (!Double.isFinite(this.maxValue)) {
            double delta = currentValue - this.minValue + deltaSmoothing;
            final double randomGene = currentValue + random.nextGaussian() * delta / 2;
            return randomGene < this.minValue ?
                    2 * this.minValue - randomGene :
                    randomGene;
        }
        return random.nextDouble() * (this.maxValue - this.minValue) + this.minValue;
    }

    public void setRandomValue(Individual individual, Random random) {
        double randomValue = this.createRandomValue(random);
        individual.setGene(this.index, randomValue, Double.NaN);
    }

    public int getIndex() {
        return index;
    }

    @Override
    public String toString() {
        return "Variable[id=" + id + ", index=" + index + ']';
    }
}
