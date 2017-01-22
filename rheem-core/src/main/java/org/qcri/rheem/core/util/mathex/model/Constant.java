package org.qcri.rheem.core.util.mathex.model;

import org.qcri.rheem.core.util.mathex.Context;
import org.qcri.rheem.core.util.mathex.Expression;

/**
 * A constant {@link Expression}.
 */
public class Constant implements Expression {

    final double value;

    public Constant(double value) {
        this.value = value;
    }

    public double getValue() {
        return this.value;
    }

    @Override
    public double evaluate(Context context) {
        return this.value;
    }

    @Override
    public Expression specify(Context context) {
        return this;
    }

    @Override
    public String toString() {
        return Double.toString(this.value);
    }
}
