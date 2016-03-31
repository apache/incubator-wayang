package org.qcri.rheem.core.util;

import org.apache.commons.lang3.Validate;

import java.io.File;
import java.net.URI;
import java.net.URL;

/**
 * Utilities for reflection code.
 */
public class Reflection {

    /**
     * Identifies and returns the JAR file declaring the {@link Class} of the given {@code object}.
     */
    public static File getDeclaringJar(Object object) {
        Validate.notNull(object);

        return getDeclaringJar(object.getClass());
    }

    /**
     * Identifies and returns the JAR file declaring the given {@link Class}.
     */
    public static File getDeclaringJar(Class<?> cls) {
        try {
            final URL location = cls.getProtectionDomain().getCodeSource().getLocation();
            final URI uri = location.toURI();
            final File file = new File(uri);
            if (!file.getPath().endsWith(".jar")) {
                throw new IllegalStateException(String.format("Class %s is not loaded from a JAR file.", cls));
            }
            return file;
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format("Could not determine JAR file declaring %s.", cls), e);
        }
    }

}
