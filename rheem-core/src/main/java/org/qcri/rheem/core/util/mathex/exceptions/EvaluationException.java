package org.qcri.rheem.core.util.mathex.exceptions;


import org.qcri.rheem.core.util.mathex.Context;
import org.qcri.rheem.core.util.mathex.Expression;

/**
 * This exception signals a failed {@link Expression} evaluation.
 *
 * @see Expression#evaluate(Context)
 */
public class EvaluationException extends MathExException {

    public EvaluationException() {
    }

    public EvaluationException(String message) {
        super(message);
    }

    public EvaluationException(String message, Throwable cause) {
        super(message, cause);
    }

    public EvaluationException(Throwable cause) {
        super(cause);
    }

    public EvaluationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
