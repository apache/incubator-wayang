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
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
            new Tuple<>(byte.class, () -> (byte) 0),
            new Tuple<>(Integer.class, () -> (byte) 0),
            new Tuple<>(short.class, () -> (short) 0),
            new Tuple<>(Short.class, () -> (short) 0),
            new Tuple<>(int.class, () -> 0),
            new Tuple<>(Integer.class, () -> 0),
            new Tuple<>(long.class, () -> 0L),
            new Tuple<>(Long.class, () -> 0L),
            new Tuple<>(boolean.class, () -> false),
            new Tuple<>(Boolean.class, () -> false),
            new Tuple<>(float.class, () -> 0f),
            new Tuple<>(Float.class, () -> 0f),
            new Tuple<>(double.class, () -> 0d),
            new Tuple<>(Double.class, () -> 0d),
            new Tuple<>(char.class, () -> '\0'),
            new Tuple<>(Character.class, () -> '\0'),
            new Tuple<>(String.class, () -> "")
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

    /**
     * Retrieve the {@link Type}s of type parameters from an extended/implemented superclass/interface.
     *
     * @param subclass   the {@link Class} implementing the superclass/interface with type parameters
     * @param superclass the superclass/interface defining the type parameters
     * @return a {@link Map} mapping the type parameter names to their implemented {@link Type}
     */
    public static Map<String, Type> getTypeParameters(Class<?> subclass, Class<?> superclass) {
        // Gather implemented interfaces and superclass.
        List<Type> genericSupertypes = Stream.concat(
                Stream.of(subclass.getGenericSuperclass()), Stream.of(subclass.getGenericInterfaces())
        ).collect(Collectors.toList());

        for (Type supertype : genericSupertypes) {

            if (supertype instanceof Class<?>) {
                // If the supertype is a Class, there are no type parameters to worry about.
                Class<?> cls = (Class<?>) supertype;
                if (!superclass.isAssignableFrom(cls)) continue;
                return getTypeParameters(cls, superclass);

            } else if (supertype instanceof ParameterizedType) {
                // Handle type parameters.
                ParameterizedType parameterizedType = (ParameterizedType) supertype;
                final Type rawType = parameterizedType.getRawType();
                if (!(rawType instanceof Class<?>)) continue;
                Class<?> cls = (Class<?>) rawType;
                if (!superclass.isAssignableFrom(cls)) continue;
                final Map<String, Type> localTypeArguments = getTypeArguments(parameterizedType);

                if (cls.equals(superclass)) {
                    // If we reached the superclass, we are good.
                    return localTypeArguments;

                } else {
                    // If we are not there yet, we need to consider to "redirect" type parameters.
                    final Map<String, Type> preliminaryResult = getTypeParameters(cls, superclass);
                    final Map<String, Type> result = new HashMap<>(preliminaryResult.size());
                    for (Map.Entry<String, Type> entry : preliminaryResult.entrySet()) {
                        if (entry.getValue() instanceof TypeVariable<?>) {
                            final Type translatedTypeArgument = localTypeArguments.getOrDefault(
                                    ((TypeVariable) entry.getValue()).getName(),
                                    entry.getValue()
                            );
                            result.put(entry.getKey(), translatedTypeArgument);
                        } else {
                            result.put(entry.getKey(), entry.getValue());
                        }
                    }
                    return result;

                }
            }
        }

        return Collections.emptyMap();
    }

    /**
     * Put the {@link TypeVariable}s of a {@link ParameterizedType} into a {@link Map}.
     *
     * @param type the {@link ParameterizedType}
     * @return the {@link Map} with the type parameter names as keys
     */
    private static Map<String, Type> getTypeArguments(ParameterizedType type) {
        final Type[] typeArguments = type.getActualTypeArguments();
        final Class<?> rawType = (Class<?>) type.getRawType();
        final TypeVariable<? extends Class<?>>[] typeParameters = rawType.getTypeParameters();
        Map<String, Type> result = new HashMap<>(typeArguments.length);
        for (int i = 0; i < typeArguments.length; i++) {
            final TypeVariable<?> typeParameter = typeParameters[i];
            final Type typeArgument = typeArguments[i];
            result.put(typeParameter.getName(), typeArgument);
        }
        return result;
    }

    /**
     * Retrieve a property from an object.
     *
     * @param obj      from which the property should be retrieved
     * @param property the property
     * @param <T>      the return type
     * @return the property
     */
    @SuppressWarnings("unchecked")
    public static <T> T getProperty(Object obj, String property) {
        Class<?> artifactClass = obj.getClass();
        String accessorName = "get" + Character.toUpperCase(property.charAt(0)) + property.substring(1);
        try {
            do {
                final Optional<Method> optMethod = Arrays.stream(artifactClass.getMethods())
                        .filter(method -> method.getName().equals(accessorName))
                        .findAny();
                if (optMethod.isPresent()) {
                    return (T) artifactClass.getMethod(accessorName).invoke(obj);
                }
                artifactClass = artifactClass.getSuperclass();
            } while (artifactClass != null);
        } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new RuntimeException(String.format("Could not execute %s() on %s.", accessorName, obj), e);
        }
        throw new IllegalArgumentException(String.format("Did not find method %s() for %s.", accessorName, obj));
    }

    /**
     * Convert the given {@link Object} to a {@code double}.
     *
     * @param o the {@link Object}
     * @return the {@code double}
     */
    public static double toDouble(Object o) {
        if (o instanceof Double) return (Double) o;
        else if (o instanceof Integer) return (Integer) o;
        else if (o instanceof Float) return (Float) o;
        else if (o instanceof Long) return (Long) o;
        else if (o instanceof Short) return (Short) o;
        else if (o instanceof Byte) return (Byte) o;
        else if (o instanceof BigDecimal) return ((BigDecimal) o).doubleValue();
        else if (o instanceof BigInteger) return ((BigInteger) o).doubleValue();
        throw new IllegalStateException(String.format("%s (%s) cannot be retrieved as double.", o,
                o == null ? "unknown class" : o.getClass().getCanonicalName()));

    }


    public static Type getWrapperClass(Type type, int index) {
        if (type != null && (type instanceof ParameterizedType)) {
            ParameterizedType parameterizedType = (ParameterizedType) type;
            return parameterizedType.getActualTypeArguments()[index];
        }
        return null;
    }
}
