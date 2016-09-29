package org.qcri.rheem.core.profiling;

import de.hpi.isg.profiledb.store.model.Measurement;
import de.hpi.isg.profiledb.store.model.Type;

/**
 * This measurement captures execution costs w.r.t. to Rheem's cost model.
 */
@Type("cost")
public class CostMeasurement extends Measurement {

    /**
     * The cost is generally captures as bounding box and this is the lower or upper value, respectively.
     */
    private double lowerCost, upperCost;

    /**
     * The probability of the cost measurement being correct.
     */
    private double probability;

    /**
     * Creates a new instance.
     *
     * @param id          the ID of the instance
     * @param lowerCost   the lower bound of the cost
     * @param upperCost   the upper bound of the cost
     * @param probability the probability of the actual cost being within the bounds
     */
    public CostMeasurement(String id, double lowerCost, double upperCost, double probability) {
        super(id);
        this.lowerCost = lowerCost;
        this.upperCost = upperCost;
        this.probability = probability;
    }

    /**
     * Deserialization constructor.
     */
    protected CostMeasurement() {
    }

    public double getLowerCost() {
        return this.lowerCost;
    }

    public void setLowerCost(double lowerCost) {
        this.lowerCost = lowerCost;
    }

    public double getUpperCost() {
        return this.upperCost;
    }

    public void setUpperCost(double upperCost) {
        this.upperCost = upperCost;
    }

    public double getProbability() {
        return this.probability;
    }

    public void setProbability(double probability) {
        this.probability = probability;
    }
}
