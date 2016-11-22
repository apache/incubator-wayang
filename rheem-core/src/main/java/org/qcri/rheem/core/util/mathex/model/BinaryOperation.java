package org.qcri.rheem.core.util.mathex.model;


import org.qcri.rheem.core.util.mathex.Context;
import org.qcri.rheem.core.util.mathex.Expression;
import org.qcri.rheem.core.util.mathex.exceptions.EvaluationException;

/**
 * An operation {@link Expression}.
 */
public class BinaryOperation implements Expression {

    private final char operator;

    private final Expression operand0, operand1;

    public BinaryOperation(Expression operand0, char operator, Expression operand1) {
        this.operand0 = operand0;
        this.operator = operator;
        this.operand1 = operand1;
    }

    @Override
    public double evaluate(Context context) {
        switch (this.operator) {
            case '+':
                return this.operand0.evaluate(context) + this.operand1.evaluate(context);
            case '-':
                return this.operand0.evaluate(context) - this.operand1.evaluate(context);
            case '*':
                return this.operand0.evaluate(context) * this.operand1.evaluate(context);
            case '/':
                return this.operand0.evaluate(context) / this.operand1.evaluate(context);
            case '%':
                return this.operand0.evaluate(context) % this.operand1.evaluate(context);
            case '^':
                return Math.pow(this.operand0.evaluate(context), this.operand1.evaluate(context));
            default:
                throw new EvaluationException(String.format("Unknown operator: \"%s\"", this.operator));
        }
    }

    @Override
    public Expression specify(Context context) {
        final Expression defaultSpecification = Expression.super.specify(context);
        if (defaultSpecification == this) {
            final Expression specifiedOperand0 = this.operand0.specify(context);
            final Expression specifiedOperand1 = this.operand1.specify(context);
            if (specifiedOperand0 != this.operand0 || specifiedOperand1 != this.operand1) {
                return new BinaryOperation(specifiedOperand0, this.operator, specifiedOperand1);
            }
        }
        return defaultSpecification;
    }

    @Override
    public String toString() {
        return String.format("(%s)%s(%s)", this.operand0, this.operator, this.operand1);
    }

}