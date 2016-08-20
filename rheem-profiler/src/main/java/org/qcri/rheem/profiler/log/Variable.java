package org.qcri.rheem.profiler.log;

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
        if (!Double.isFinite(this.minValue)) {
            if (!Double.isInfinite(this.maxValue)) {
                return currentValue + random.nextGaussian() * Math.abs(currentValue);
            } else {
                double delta = this.maxValue - currentValue;
                return this.maxValue - Math.abs(random.nextGaussian()) * delta;
            }
        } else if (!Double.isFinite(this.maxValue)) {
            double delta = currentValue - this.minValue;
            return this.minValue + Math.abs(random.nextGaussian()) * delta;
        }
        return random.nextDouble() * (this.maxValue - this.minValue) + this.minValue;
    }

    public void setRandomValue(Individual individual, Random random) {
        double randomValue = this.createRandomValue(random);
        individual.getGenome()[this.index] = randomValue;
    }

    public int getIndex() {
        return index;
    }
}
