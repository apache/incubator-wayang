package org.qcri.rheem.core.util.mathex;

import org.qcri.rheem.core.util.mathex.exceptions.EvaluationException;

import java.util.HashMap;
import java.util.Map;
import java.util.function.ToDoubleFunction;

/**
 * Default {@link Context} implementation that can be configured.
 */
public class DefaultContext implements Context {

    private Context parentContext;

    private Map<String, Double> variableValues = new HashMap<>();

    private Map<String, ToDoubleFunction<double[]>> functions = new HashMap<>();

    /**
     * Creates a new instance.
     */
    public DefaultContext() {
        this(null);
    }

    /**
     * Creates a new instance with a parent. If the new instance cannot serve a contextual information, it will
     * ask the parent.
     *
     * @param parentContext the parent instance
     */
    public DefaultContext(Context parentContext) {
        this.parentContext = parentContext;
    }

    @Override
    public double getVariable(String variableName) {
        Double variableValue = this.variableValues.get(variableName);
        if (variableValue == null && this.parentContext != null) {
            variableValue = this.parentContext.getVariable(variableName);
        }
        if (variableValue == null) {
            throw new EvaluationException(String.format("No variable named \"%s\".", variableName));
        }
        return variableValue;
    }

    @Override
    public ToDoubleFunction<double[]> getFunction(String functionName) {
        ToDoubleFunction<double[]> function = this.functions.get(functionName);
        if (function == null && this.parentContext != null) {
            function = this.parentContext.getFunction(functionName);
        }
        if (function == null) {
            throw new EvaluationException(String.format("No function named \"%s\".", functionName));
        }
        return function;
    }

    /**
     * Set the parent context.
     *
     * @param parentContext the parent context
     */
    public void setParentContext(Context parentContext) {
        this.parentContext = parentContext;
    }

    /**
     * Register a variable.
     *
     * @param name  the name of the variable
     * @param value the value
     */

    public void setVariable(String name, double value) {
        this.variableValues.put(name, value);
    }

    /**
     * Register a function.
     *
     * @param name     the name of the function
     * @param function the function
     */
    public void setFunction(String name, ToDoubleFunction<double[]> function) {
        this.functions.put(name, function);
    }

    /**
     * Create an instance with standard variables and functions.
     *
     * @return the instance
     */
    static Context createBaseContext() {
        DefaultContext baseContext = new DefaultContext();
        baseContext.setFunction("ln", args -> Math.log(args[0]));
        baseContext.setFunction("ld", args -> Math.log(args[0]) / Math.log(2));
        baseContext.setFunction("log", args -> Math.log(args[0]) / Math.log(args[1]));
        baseContext.setFunction("max", args -> {
            if (args.length == 0) throw new EvaluationException("max(...) requires at least one argument.");
            double max = args[0];
            for (int i = 0; i < args.length; i++) max = Math.max(max, args[i]);
            return max;
        });
        baseContext.setFunction("min", args -> {
            if (args.length == 0) throw new EvaluationException("min(...) requires at least one argument.");
            double min = args[0];
            for (int i = 0; i < args.length; i++) min = Math.min(min, args[i]);
            return min;
        });
        baseContext.setFunction("abs", args -> Math.abs(args[0]));

        baseContext.setVariable("pi", Math.PI);
        baseContext.setVariable("e", Math.E);

        return baseContext;
    }
}
