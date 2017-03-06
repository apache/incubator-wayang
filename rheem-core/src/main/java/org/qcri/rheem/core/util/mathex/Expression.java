package org.qcri.rheem.core.util.mathex;


import org.qcri.rheem.core.util.mathex.exceptions.EvaluationException;
import org.qcri.rheem.core.util.mathex.model.Constant;

/**
 * A mathematical expression that can be evaluated.
 */
public interface Expression {

    /**
     * Parse the given {@code specification} and evaluate it with the {@link Context#baseContext}.
     *
     * @param specification a mathematical expression
     * @return the result of the evaluation
     * @throws EvaluationException if the evaluation failed
     */
    static double evaluate(String specification) throws EvaluationException {
        final Expression expression = ExpressionBuilder.parse(specification);
        return expression.evaluate(Context.baseContext);
    }

    /**
     * Parse the given {@code specification} and evaluate it with the given {@link Context}.
     *
     * @param specification a mathematical expression
     * @param context       provides contextual information
     * @return the result of the evaluation
     * @throws EvaluationException if the evaluation failed
     */
    static double evaluate(String specification, Context context) throws EvaluationException {
        final Expression expression = ExpressionBuilder.parse(specification);
        return expression.evaluate(context);
    }

    double evaluate(Context context) throws EvaluationException;

    /**
     * Turn this dynamic instance into a more static one by directly incorporating the given {@link Context}.
     *
     * @param context provides contextual information to be weaved in
     * @return the specified instance
     */
    default Expression specify(Context context) {
        // Default strategy: try to evaluate this instance and return a constant.
        try {
            final double result = this.evaluate(context);
            return new Constant(result);
        } catch (EvaluationException e) {
            return this;
        }
    }

}
