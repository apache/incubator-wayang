package org.apache.wayang.core.util.mathex.exceptions;

import org.apache.wayang.core.util.mathex.Context;
import org.apache.wayang.core.util.mathex.Expression;

/**
 * This exception signals a failed {@link Expression} evaluation.
 *
 * @see Expression#evaluate(Context)
 */
public class MathExException extends RuntimeException {

    public MathExException() {
    }

    public MathExException(String message) {
        super(message);
    }

    public MathExException(String message, Throwable cause) {
        super(message, cause);
    }

    public MathExException(Throwable cause) {
        super(cause);
    }

    public MathExException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
