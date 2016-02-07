package org.qcri.rheem.core.platform;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;

/**
 * A platform describes an execution engine that executes {@link ExecutionOperator}s.
 */
public abstract class Platform {

    private final String name;

    public static Platform load(String platformClassName) {
        final Class<?> platformClass;
        try {
            platformClass = Class.forName(platformClassName);
            final Method getInstanceMethod = platformClass.getMethod("getInstance");
            return (Platform) getInstanceMethod.invoke(null);
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RheemException("Could not load platform: " + platformClassName, e);
        }
    }

    public Platform(String name) {
        this.name = name;
    }

    /**
     * Dummy interface for executing plans. This will definitively change in the real implementation.
     *
     * @param executionOperator the execution operator whose result should be evaluated
     */
    public void evaluate(ExecutionOperator executionOperator) {
        Validate.isTrue(this.isExecutable());
        final Executor executor = this.getExecutorFactory().create();
        executor.evaluate(executionOperator);
    }

    public abstract Executor.Factory getExecutorFactory();

    public abstract Collection<Mapping> getMappings();

    public String getName() {
        return name;
    }

    public abstract boolean isExecutable();

    // TODO: Return some more descriptors about the state of the platform (e.g., available machines, RAM, ...)

    @Override
    public String toString() {
        return String.format("Platform[%s]", this.getName());
    }
}
