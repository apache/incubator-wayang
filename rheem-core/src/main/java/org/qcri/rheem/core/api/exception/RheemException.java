package org.qcri.rheem.core.api.exception;

/**
 * Exception that declares a problem of Rheem.
 */
public class RheemException extends RuntimeException {

    public RheemException() {
    }

    public RheemException(String message) {
        super(message);
    }

    public RheemException(String message, Throwable cause) {
        super(message, cause);
    }

    public RheemException(Throwable cause) {
        super(cause);
    }

    public RheemException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
