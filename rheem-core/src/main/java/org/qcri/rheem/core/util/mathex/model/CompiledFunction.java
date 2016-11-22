package org.qcri.rheem.core.util.mathex.model;

import org.qcri.rheem.core.util.mathex.Context;
import org.qcri.rheem.core.util.mathex.Expression;

import java.util.ArrayList;
import java.util.List;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;

/**
 * {@link Expression} implementation that represents a function with a static implementation.
 */
public class CompiledFunction implements Expression {

    /**
     * The name of the function.
     */
    final String name;

    /**
     * The implementation of this instance.
     */
    final ToDoubleFunction<double[]> implementation;

    /**
     * The argument {@link Expression}s.
     */
    final List<Expression> arguments;

    public CompiledFunction(String name, ToDoubleFunction<double[]> implementation, List<Expression> arguments) {
        this.name = name;
        this.implementation = implementation;
        this.arguments = arguments;
    }

    @Override
    public double evaluate(Context context) {
        // Evaluate the arguments.
        double[] args = new double[this.arguments.size()];
        int i = 0;
        for (Expression argument : this.arguments) {
            args[i++] = argument.evaluate(context);
        }

        // Apply the function.
        return this.implementation.applyAsDouble(args);
    }

    @Override
    public Expression specify(Context context) {
        final Expression specification = Expression.super.specify(context);
        if (specification == this) {
            List<Expression> specifiedArgs = new ArrayList<>(this.arguments.size());
            boolean isAnySpecified = false;
            for (Expression argument : this.arguments) {
                final Expression specifiedArg = argument.specify(context);
                isAnySpecified |= specifiedArg != argument;
                specifiedArgs.add(specifiedArg);
            }
            if (isAnySpecified) {
                return new CompiledFunction(this.name, this.implementation, specifiedArgs);
            }
        }
        return specification;
    }

    @Override
    public String toString() {
        return this.name + this.arguments.stream().map(Object::toString).collect(Collectors.joining(", ", "(", ")"));
    }
}
