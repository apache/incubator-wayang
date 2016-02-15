package org.qcri.rheem.core.platform;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ChannelInitializer;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.util.Tuple;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

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

    /**
     * <i>Shortcut.</i> Creates an {@link Executor} using the {@link #getExecutorFactory()}.
     *
     * @return the {@link Executor}
     */
    public Executor createExecutor() {
        Validate.isTrue(this.isExecutable());
        return this.getExecutorFactory().create();
    }

    public abstract Executor.Factory getExecutorFactory();

    public abstract Collection<Mapping> getMappings();

    public String getName() {
        return this.name;
    }

    public abstract boolean isExecutable();

    /**
     * If this instance provides {@link ExecutionOperator}s, then this method provides the {@link ChannelInitializer}s
     * to connect them.
     *
     * @return the appropriate {@link ChannelInitializer} for the given {@link Channel} type or {@code null} if none
     */
    public abstract <T extends Channel> ChannelInitializer<T> getChannelInitializer(Class<T> channelClass);

    // TODO: Return some more descriptors about the state of the platform (e.g., available machines, RAM, ...)

    public Tuple<Class<? extends Channel>[], Class<? extends Channel>[]>
    pickChannelClasses(ExecutionOperator operator,
                       int outputIndex,
                       List<InputSlot<Object>> internalInputs,
                       List<InputSlot<Object>> externalInputs) {
        // NB: Default implementation. Override as required.

        // Gather all supported output Channel classes.
        final List<Class<? extends Channel>> supportedOutputChannels = operator.getSupportedOutputChannels(outputIndex);

        // Try to find a common Channel for all external inputs.
        final Class<? extends Channel>[] externalChannels = pickChannelClasses(
                supportedOutputChannels, externalInputs, false);
        if (externalChannels == null) return null;

        // Determine if we need reusable channels.
        boolean isRequestReusable = (externalChannels.length == 0 && internalInputs.size() < 2) ||
                (this.hasOnlySingleClass(externalChannels) && internalInputs.isEmpty());

        final Class<? extends Channel>[] internalChannels = pickChannelClasses(
                supportedOutputChannels, internalInputs, isRequestReusable);
        if (internalChannels == null) {
            return null;
        }

        return new Tuple<>(internalChannels, externalChannels);
    }

    private Class<? extends Channel>[] pickChannelClasses(List<Class<? extends Channel>> outputChannelClasses,
                                                          List<InputSlot<Object>> inputs,
                                                          boolean isRequestReusable) {
        List<Class<? extends Channel>> permittedChannels = isRequestReusable ?
                outputChannelClasses.stream()
                        .filter(channelClass -> this.getChannelInitializer(channelClass).isReusable())
                        .collect(Collectors.toList()) :
                outputChannelClasses;
        final List<List<Class<? extends Channel>>> inputChannelClassLists = inputs.stream()
                .map(input -> ((ExecutionOperator) input.getOwner()).getSupportedInputChannels(input.getIndex()))
                .collect(Collectors.toList());
        final Class<? extends Channel>[] pickedChannels = this.pickChannelClasses(outputChannelClasses, inputChannelClassLists);
        if (pickedChannels == null) {
            return null;
        }
        return pickedChannels;
    }

    /**
     * Designates a {@link Channel} class for each entry in {@code inputChannelClassLists}.
     *
     * @param outputChannelClasses   {@link Channel} classes that may be picked; ordered by preference
     * @param inputChannelClassLists {@link List}s of {@link Channel}s; for each, one{@link Channel} should be picked
     * @return an array containing the picked classes, aligned with {@code inputChannelClassLists}, or {@code null}
     * if a full match was not possible
     */
    protected Class<? extends Channel>[] pickChannelClasses(List<Class<? extends Channel>> outputChannelClasses,
                                                            List<List<Class<? extends Channel>>> inputChannelClassLists) {

        // Keep track of the picked classes and how often it has been picked.
        // NB: This greedy algorithm might be improved.
        Class<? extends Channel>[] pickedChannelClasses = new Class[inputChannelClassLists.size()];
        int[] maxMatches = new int[inputChannelClassLists.size()];
        for (Class<? extends Channel> supportedOutputChannel : outputChannelClasses) {
            boolean[] matches = new boolean[inputChannelClassLists.size()];
            int numMatches = 0;
            for (int i = 0; i < inputChannelClassLists.size(); i++) {
                final List<Class<? extends Channel>> classes = inputChannelClassLists.get(i);
                if ((matches[i] = classes.contains(supportedOutputChannel))) {
                    numMatches++;
                }
            }
            for (int i = 0; i < inputChannelClassLists.size(); i++) {
                if (matches[i] && (pickedChannelClasses[i] == null || numMatches > maxMatches[i])) {
                    pickedChannelClasses[i] = supportedOutputChannel;
                    maxMatches[i] = numMatches;
                }
            }
        }

        for (Class<? extends Channel> pickedChannelClass : pickedChannelClasses) {
            if (pickedChannelClass == null) return null;
        }

        return pickedChannelClasses;
    }

    protected boolean hasOnlySingleClass(Class[] array) {
        for (int i = 1; i < array.length; i++) {
            if (array[0] != array[i]) return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return String.format("Platform[%s]", this.getName());
    }

}
