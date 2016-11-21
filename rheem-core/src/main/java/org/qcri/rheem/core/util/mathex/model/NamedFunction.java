package org.qcri.rheem.core.util.mathex.model;

import org.qcri.rheem.core.util.mathex.Context;
import org.qcri.rheem.core.util.mathex.Expression;
import org.qcri.rheem.core.util.mathex.exceptions.EvaluationException;

import java.util.List;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;

/**
 * {@link Expression} implementation that represents a function that is identified
 * via its name.
 */
public class NamedFunction implements Expression {

    /**
     * The name of the function.
     */
    final String name;

    /**
     * The argument {@link Expression}s.
     */
    final List<Expression> arguments;

    public NamedFunction(String name, List<Expression> arguments) {
        this.name = name;
        this.arguments = arguments;
    }

    @Override
    public double evaluate(Context context) {
        // Determine the function.
        final ToDoubleFunction<double[]> implementation = context.getFunction(this.name);

        // Evaluate the arguments.
        double[] args = new double[this.arguments.size()];
        int i = 0;
        for (Expression argument : this.arguments) {
            args[i++] = argument.evaluate(context);
        }

        // Apply the function.
        return implementation.applyAsDouble(args);
    }

    @Override
    public Expression specify(Context context) {
        Expression defaultSpecification = Expression.super.specify(context);
        if (defaultSpecification == this) {
            try {
                final ToDoubleFunction<double[]> implementation = context.getFunction(this.name);
                final List<Expression> specifiedArgs = this.arguments.stream()
                        .map(argument -> argument.specify(context))
                        .collect(Collectors.toList());
                return new CompiledFunction(this.name, implementation, specifiedArgs);
            } catch (EvaluationException e) {
            }
        }
        return defaultSpecification;
    }

    @Override
    public String toString() {
        return this.name + this.arguments.stream().map(Object::toString).collect(Collectors.joining(", ", "(", ")"));
    }
}
