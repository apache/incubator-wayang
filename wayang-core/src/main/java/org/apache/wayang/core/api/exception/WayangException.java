package org.apache.wayang.core.api.exception;

/**
 * Exception that declares a problem of Wayang.
 */
public class WayangException extends RuntimeException {

    public WayangException() {
    }

    public WayangException(String message) {
        super(message);
    }

    public WayangException(String message, Throwable cause) {
        super(message, cause);
    }

    public WayangException(Throwable cause) {
        super(cause);
    }

    public WayangException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
