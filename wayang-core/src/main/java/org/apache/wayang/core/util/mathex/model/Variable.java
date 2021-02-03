package org.apache.wayang.core.util.mathex.model;


import org.apache.wayang.core.util.mathex.Context;
import org.apache.wayang.core.util.mathex.Expression;

/**
 * A variable {@link Expression}
 */
public class Variable implements Expression {

    final String name;

    public Variable(String name) {
        this.name = name;
    }

    @Override
    public double evaluate(Context context) {
        return context.getVariable(this.name);
    }

    @Override
    public String toString() {
        return this.name;
    }
}
