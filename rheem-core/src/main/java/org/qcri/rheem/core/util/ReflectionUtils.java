package org.qcri.rheem.core.util;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.net.URL;

/**
 * Utilities for reflection code.
 */
public class ReflectionUtils {

    private static final Logger logger = LoggerFactory.getLogger(ReflectionUtils.class);

    /**
     * Identifies and returns the JAR file declaring the {@link Class} of the given {@code object} or {@code null} if
     * no such file could be determined.
     */
    public static String getDeclaringJar(Object object) {
        Validate.notNull(object);

        return getDeclaringJar(object.getClass());
    }

    /**
     * Identifies and returns the JAR file declaring the given {@link Class} if no such file could be determined.
     */
    public static String getDeclaringJar(Class<?> cls) {
        try {
            final URL location = cls.getProtectionDomain().getCodeSource().getLocation();
            final URI uri = location.toURI();
            final String path = uri.getPath();
            if (path.endsWith(".jar")) {
                return path;
            } else {
                logger.warn("Class {} is not loaded from a JAR file, but from {}.", cls, path);
            }
        } catch (Exception e) {
            logger.error(String.format("Could not determine JAR file declaring %s.", cls), e);
        }
        return null;
    }

}
