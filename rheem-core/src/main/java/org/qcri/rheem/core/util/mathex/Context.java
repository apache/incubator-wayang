package org.qcri.rheem.core.util.mathex;

import org.qcri.rheem.core.util.mathex.exceptions.EvaluationException;

import java.util.function.ToDoubleFunction;

/**
 * Provides contextual variables and functions in order to evaluate an {@link Expression}.
 */
public interface Context {

    Context baseContext = DefaultContext.createBaseContext();


    /**
     * Provide the value for a variable.
     *
     * @param variableName the name of the variable
     * @return the variable value
     * @throws EvaluationException if the variable request could not be served
     */
    double getVariable(String variableName) throws EvaluationException;

    /**
     * Provide the function.
     *
     * @param functionName the name of the function
     * @return the function
     * @throws EvaluationException if the variable request could not be served
     */
    ToDoubleFunction<double[]> getFunction(String functionName) throws EvaluationException;

}
