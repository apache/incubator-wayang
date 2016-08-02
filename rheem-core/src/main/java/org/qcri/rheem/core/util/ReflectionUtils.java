package org.qcri.rheem.core.util;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.exception.RheemException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utilities for reflection code.
 */
public class ReflectionUtils {

    private static final Logger logger = LoggerFactory.getLogger(ReflectionUtils.class);

    private static final Pattern defaultConstructorPattern = Pattern.compile(
            "new ([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)\\(\\)"
    );

    private static final Pattern arglessMethodPattern = Pattern.compile(
            "([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)\\.([a-zA-Z_][a-zA-Z0-9_]*)\\(\\)"
    );

    private static final Pattern constantPattern = Pattern.compile(
            "([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)\\.([a-zA-Z_][a-zA-Z0-9_]*)"
    );

    private static final List<Tuple<Class<?>, Supplier<?>>> defaultParameterSuppliers = Arrays.asList(
            new Tuple<>(int.class, () -> 0),
            new Tuple<>(Integer.class, () -> 0),
            new Tuple<>(long.class, () -> 0L),
            new Tuple<>(Long.class, () -> 0L)
    );

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
                logger.warn("Class {} is not loaded from a JAR file, but from {}. Thus, cannot provide the JAR file.", cls, path);
            }
        } catch (Exception e) {
            logger.error(String.format("Could not determine JAR file declaring %s.", cls), e);
        }
        return null;
    }

    /**
     * Provides a resource as an {@link InputStream}.
     *
     * @param resourceName the name or path of the resource
     * @return an {@link InputStream} with the contents of the resource or {@code null} if the resource could not be found
     */
    public static InputStream loadResource(String resourceName) {
        return Thread.currentThread().getContextClassLoader().getResourceAsStream(resourceName);
    }

    /**
     * Provides a resource as {@link URL}.
     *
     * @param resourceName the name or path of the resource
     * @return a {@link URL} describing the path to the resource or {@code null} if the resource could not be found
     */
    public static URL getResourceURL(String resourceName) {
        return Thread.currentThread().getContextClassLoader().getResource(resourceName);
    }

    /**
     * Casts the given {@link Class} to a more specific one.
     *
     * @param baseClass that should be casted
     * @param <T>       the specific type parameter for the {@code baseClass}
     * @return the {@code baseClass}, casted
     */
    @SuppressWarnings("unchecked")
    public static <T> Class<T> specify(Class<? super T> baseClass) {
        return (Class<T>) baseClass;
    }

    /**
     * Casts the given {@link Class} to a more general one.
     *
     * @param baseClass that should be casted
     * @param <T>       the specific type parameter for the {@code baseClass}
     * @return the {@code baseClass}, casted
     */
    @SuppressWarnings("unchecked")
    public static <T> Class<T> generalize(Class<? extends T> baseClass) {
        return (Class<T>) baseClass;
    }

    /**
     * Tries to evaluate the given statement, thereby supporting the following statement types:
     * <ul>
     * <li>{@code new <class name>()}</li>
     * <li>{@code <class name>.<constant>}</li>
     * <li>{@code <class name>.<static method>}()</li>
     * </ul>
     *
     * @param statement the statement
     * @param <T>       the return type
     * @return the result of the evaluated statement
     */
    public static <T> T evaluate(String statement) throws IllegalArgumentException {
        statement = statement.trim();
        Matcher matcher = defaultConstructorPattern.matcher(statement);
        if (matcher.matches()) {
            try {
                return instantiateDefault(matcher.group(1));
            } catch (Exception e) {
                throw new IllegalArgumentException(String.format("Could not instantiate '%s'.", statement), e);
            }
        }

        matcher = arglessMethodPattern.matcher(statement);
        if (matcher.matches()) {
            try {
                return executeStaticArglessMethod(matcher.group(1), matcher.group(2));
            } catch (Exception e) {
                throw new IllegalArgumentException(String.format("Could not execute '%s'.", statement), e);
            }
        }

        matcher = constantPattern.matcher(statement);
        if (matcher.matches()) {
            try {
                return retrieveStaticVariable(matcher.group(1), matcher.group(2));
            } catch (Exception e) {
                throw new IllegalArgumentException(String.format("Could not execute '%s'.", statement), e);
            }
        }

        throw new IllegalArgumentException(String.format("Unknown expression type: '%s'.", statement));
    }

    /**
     * Executes a parameterless static method.
     *
     * @param className  the name of the {@link Class} that comprises the method
     * @param methodName the name of the method
     * @param <T>
     * @return the return value of the executed method
     */
    @SuppressWarnings("unchecked")
    public static <T> T executeStaticArglessMethod(String className, String methodName) {
        try {
            final Class<?> cls = Class.forName(className);
            final Method method = cls.getMethod(methodName);
            Validate.isTrue(method.getParameters().length == 0, "Method has parameters.");
            return (T) method.invoke(null);
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new IllegalArgumentException(String.format("Could not execute %s.%s().", className, methodName), e);
        }
    }

    /**
     * Retrieves a static variable.
     *
     * @param className    the name of the {@link Class} that comprises the method
     * @param variableName the name of the method
     * @param <T>
     * @return the return value of the executed method
     */
    @SuppressWarnings("unchecked")
    public static <T> T retrieveStaticVariable(String className, String variableName) {
        try {
            final Class<?> cls = Class.forName(className);
            final Field field = cls.getField(variableName);
            return (T) field.get(null);
        } catch (ClassNotFoundException | NoSuchFieldException | IllegalAccessException e) {
            throw new IllegalArgumentException(String.format("Could not retrieve %s.%s.", className, variableName), e);
        }
    }

    /**
     * Creates a new instance of a {@link Class} via the default constructor.
     *
     * @param className name of the {@link Class} to be instantiated
     * @return the instance
     */
    public static <T> T instantiateDefault(String className) {
        try {
            @SuppressWarnings("unchecked") // Will fail anyway, if incorrect.
                    Class<T> cls = (Class<T>) Class.forName(className);
            return cls.newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new RheemException("Could not instantiate class.", e);
        }
    }

    /**
     * Creates a new instance of the given {@link Class} via the default constructor.
     *
     * @param cls the {@link Class} to be instantiated
     * @return the instance
     */
    public static <T> T instantiateDefault(Class<? extends T> cls) {
        try {
            return cls.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RheemException("Could not instantiate class.", e);
        }
    }

    /**
     * Tries to instantiate an arbitrary instance of the given {@link Class}.
     *
     * @param cls               that should be instantiated
     * @param defaultParameters designate specific default parameter values for parameter {@link Class}es
     * @return the instance
     */
    @SuppressWarnings("unchecked")
    public static <T> T instantiateSomehow(Class<T> cls, Tuple<Class<?>, Supplier<?>>... defaultParameters) {
        return instantiateSomehow(cls, Arrays.asList(defaultParameters));
    }

    /**
     * Tries to instantiate an arbitrary instance of the given {@link Class}.
     *
     * @param cls               that should be instantiated
     * @param defaultParameters designate specific default parameter values for parameter {@link Class}es
     * @return the instance
     */
    @SuppressWarnings("unchecked")
    public static <T> T instantiateSomehow(Class<T> cls, List<Tuple<Class<?>, Supplier<?>>> defaultParameters) {
        try {
            for (Constructor<?> constructor : cls.getConstructors()) {
                try {
                    final Class<?>[] parameterTypes = constructor.getParameterTypes();
                    Object[] parameters = new Object[parameterTypes.length];
                    for (int i = 0; i < parameterTypes.length; i++) {
                        Class<?> parameterType = parameterTypes[i];
                        Object parameter = getDefaultParameter(parameterType, defaultParameters);
                        if (parameter == null) {
                            parameter = getDefaultParameter(parameterType, defaultParameterSuppliers);
                        }
                        parameters[i] = parameter;
                    }
                    return (T) constructor.newInstance(parameters);
                } catch (Throwable t) {
                    logger.debug("Could not instantiate {}.", cls.getSimpleName(), t);
                }
            }
        } catch (Throwable t) {
            throw new RheemException(String.format("Could not get constructors for %s.", cls.getSimpleName()));
        }

        throw new RheemException(String.format("Could not instantiate %s.", cls.getSimpleName()));
    }

    /**
     * Searches for a compatible {@link Class} in the given {@link List} (subclass or equal) for the given parameter
     * {@link Class}. If a match is found, the corresponding {@link Supplier} is used to create a default parameter.
     *
     * @param parameterClass            {@link Class} of a parameter
     * @param defaultParameterSuppliers supply default values for various parameter {@link Class}es
     * @return the first match's {@link Supplier} value or {@code null} if no match was found
     */
    private static Object getDefaultParameter(Class<?> parameterClass, List<Tuple<Class<?>, Supplier<?>>> defaultParameterSuppliers) {
        for (Tuple<Class<?>, Supplier<?>> defaultParameterSupplier : defaultParameterSuppliers) {
            if (parameterClass.isAssignableFrom(defaultParameterSupplier.getField0())) {
                return defaultParameterSupplier.getField1().get();
            }
        }
        return null;
    }
}
