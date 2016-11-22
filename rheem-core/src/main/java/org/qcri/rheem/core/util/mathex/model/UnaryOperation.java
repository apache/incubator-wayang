package org.qcri.rheem.core.util.mathex.model;

import org.qcri.rheem.core.util.mathex.Context;
import org.qcri.rheem.core.util.mathex.Expression;
import org.qcri.rheem.core.util.mathex.exceptions.EvaluationException;

/**
 * An operation {@link Expression}.
 */
public class UnaryOperation implements Expression {

    private final char operator;

    private final Expression operand;

    public UnaryOperation(char operator, Expression operand) {
        this.operator = operator;
        this.operand = operand;
    }

    @Override
    public double evaluate(Context context) {
        switch (this.operator) {
            case '+':
                return this.operand.evaluate(context);
            case '-':
                return -this.operand.evaluate(context);
            default:
                throw new EvaluationException(String.format("Unknown operator: \"%s\"", this.operator));
        }
    }

    @Override
    public String toString() {
        return String.format("%s(%s)", this.operator, this.operand);
    }

}
