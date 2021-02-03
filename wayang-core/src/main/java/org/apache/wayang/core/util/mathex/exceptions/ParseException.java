package org.apache.wayang.core.util.mathex.exceptions;

import org.apache.wayang.core.util.mathex.Context;
import org.apache.wayang.core.util.mathex.Expression;

/**
 * This exception signals a failed {@link Expression} evaluation.
 *
 * @see Expression#evaluate(Context)
 */
public class ParseException extends MathExException {

    public ParseException() {
    }

    public ParseException(String message) {
        super(message);
    }

    public ParseException(String message, Throwable cause) {
        super(message, cause);
    }

    public ParseException(Throwable cause) {
        super(cause);
    }

    public ParseException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
