package org.qcri.rheem.basic.util.Exception;

import org.qcri.rheem.core.api.exception.RheemException;

/**
 * Exception that declares a problem of JSONParser
 */
public class JSONParserException extends RheemException {
    public JSONParserException() {
    }

    public JSONParserException(String message) {
        super(message);
    }

    public JSONParserException(String message, Throwable cause) {
        super(message, cause);
    }

    public JSONParserException(Throwable cause) {
        super(cause);
    }

    public JSONParserException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
