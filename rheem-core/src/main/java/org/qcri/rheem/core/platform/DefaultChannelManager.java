package org.qcri.rheem.core.platform;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ChannelInitializer;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.util.Tuple;

import java.util.ArrayList;
import java.util.List;

/**
 * TODO
 */
public abstract class DefaultChannelManager implements ChannelManager {

    private final Platform platform;

    private final Class<? extends Channel> reusableInternalChannelInitializerClass;

    private final Class<? extends Channel> nonreusableInternalChannelInitializerClass;

    public DefaultChannelManager(Platform platform,
                                 Class<? extends Channel> reusableInternalChannelInitializerClass,
                                 Class<? extends Channel> nonreusableInternalChannelInitializerClass) {
        Validate.notNull(platform);
        this.platform = platform;
        this.reusableInternalChannelInitializerClass = reusableInternalChannelInitializerClass;
        this.nonreusableInternalChannelInitializerClass = nonreusableInternalChannelInitializerClass;
    }

    @Override
    public boolean connect(ExecutionTask sourceTask, int outputIndex, List<Tuple<ExecutionTask, Integer>> targetDescriptors) {
        final ExecutionOperator sourceOperator = sourceTask.getOperator();
        assert sourceOperator.getPlatform().equals(this.platform);

        // Gather all supported output Channel classes.
        final List<Class<? extends Channel>> supportedOutputChannels = sourceOperator.getSupportedOutputChannels(outputIndex);

        // Find the best-matching Channel class for each input.
        List<Class<? extends Channel>> pickedChannelClasses = new ArrayList<>(targetDescriptors.size());
        for (Tuple<ExecutionTask, Integer> targetDescriptor : targetDescriptors) {
            final ExecutionTask targetTask = targetDescriptor.getField0();
            final ExecutionOperator targetOperator = targetTask.getOperator();
            final Integer inputIndex = targetDescriptor.getField1();
            final List<Class<? extends Channel>> supportedInputChannels =
                    targetOperator.getSupportedInputChannels(inputIndex);

            Class<? extends Channel> pickedClass = this.pickChannelClass(supportedOutputChannels, supportedInputChannels);
            if (pickedClass == null) {
                return false;
            } else {
                pickedChannelClasses.add(pickedClass);
            }
        }

        // Find out if we need a reusable internal Channel at first. This is the case if we have multiple consumers.
        int numDistinctExternalChannels = (int) pickedChannelClasses.stream()
                .filter(pickedClass -> !this.getChannelInitializer(pickedClass).isInternal())
                .distinct()
                .count();
        int numInternalChannels = (int) pickedChannelClasses.stream()
                .filter(pickedClass -> this.getChannelInitializer(pickedClass).isInternal())
                .count();
        boolean hasInternalReusableChannel = pickedChannelClasses.stream()
                .filter(pickedClass -> this.getChannelInitializer(pickedClass).isInternal())
                .map(pickedClass -> this.getChannelInitializer(pickedClass).isReusable())
                .reduce(true, (a, b) -> a & b);
        boolean isRequestReusableInternalChannel = hasInternalReusableChannel ||
                (numDistinctExternalChannels + numInternalChannels) > 1;

        // Pick the internal channel.
        final ChannelInitializer internalChannelInitializer =
                this.getInternalChannelInitializer(isRequestReusableInternalChannel);
        internalChannelInitializer.setUpOutput(sourceTask, outputIndex);

        // Once, we have settled upon the internal Channel, let the ChannelInitializer do their work, assuming that
        // they will now incorporate it.
        for (int targetId = 0; targetId < targetDescriptors.size(); targetId++) {
            // Set up Channel as output.
            final Class<? extends Channel> pickedChannelClass = pickedChannelClasses.get(targetId);
            final ChannelInitializer sourceInitializer = this.getChannelInitializer(pickedChannelClass);
            final Channel channel = sourceInitializer.setUpOutput(sourceTask, outputIndex);

            // Connect to the target ExecutionTask.
            // TODO: Allow the target Platform to plan Channels together (e.g., reading HDFS only once).
            final Tuple<ExecutionTask, Integer> targetDescriptor = targetDescriptors.get(targetId);
            final ExecutionTask targetTask = targetDescriptor.getField0();
            final ExecutionOperator targetOperator = targetTask.getOperator();
            final Integer inputIndex = targetDescriptor.getField1();
            final ChannelManager targetChannelManager = targetOperator.getPlatform().getChannelManager();
            final ChannelInitializer targetInitializer = targetChannelManager.getChannelInitializer(channel.getClass());
            targetInitializer.setUpInput(channel, targetTask, inputIndex);
        }

        return true;
    }

    /**
     * Picks a {@link Channel} class that exists in both given {@link List}s.
     *
     * @param supportedOutputChannels a {@link List} of (output) {@link Channel} classes
     * @param supportedInputChannels  a {@link List} of (input) {@link Channel} classes
     * @return the picked {@link Channel} class or {@code null} if none was picked
     */
    protected Class<? extends Channel> pickChannelClass(List<Class<? extends Channel>> supportedOutputChannels,
                                                        List<Class<? extends Channel>> supportedInputChannels) {
        for (Class<? extends Channel> supportedOutputChannel : supportedOutputChannels) {
            if (supportedInputChannels.contains(supportedOutputChannel)) {
                return supportedOutputChannel;
            }
        }
        return null;
    }


    protected ChannelInitializer getInternalChannelInitializer(boolean isRequestReusable) {
        Class<? extends Channel> channelClass =  isRequestReusable ?
                this.reusableInternalChannelInitializerClass :
                this.nonreusableInternalChannelInitializerClass;
        return this.getChannelInitializer(channelClass);
    }


    // LEGACY CODE --- MAYBE WE NEED IT SOMETIME.

//    /**
//     * Creates a {@link List} filled with {@code null}s.
//     *
//     * @param size the size of the {@link List}
//     * @return the {@link List}
//     */
//    private List<Class<? extends Channel>> createNullList(int size) {
//        List<Class<? extends Channel>> nullList = new ArrayList<>(size);
//        for (int i = 0; i < size; i++) {
//            nullList.add(null);
//        }
//        return nullList;
//    }
//
//    public Tuple<Class<? extends Channel>[], Class<? extends Channel>[]>
//    pickChannelClasses(ExecutionOperator operator,
//                       int outputIndex,
//                       List<InputSlot<Object>> internalInputs,
//                       List<InputSlot<Object>> externalInputs) {
//        // NB: Default implementation. Override as required.
//
//        // Gather all supported output Channel classes.
//        final List<Class<? extends Channel>> supportedOutputChannels = operator.getSupportedOutputChannels(outputIndex);
//
//        // Try to find a common Channel for all external inputs.
//        final Class<? extends Channel>[] externalChannels = this.pickChannelClasses(
//                supportedOutputChannels, externalInputs, false);
//        if (externalChannels == null) return null;
//
//        // Determine if we need reusable channels.
//        boolean isRequestReusable = (externalChannels.length == 0 && internalInputs.size() < 2) ||
//                (this.hasOnlySingleClass(externalChannels) && internalInputs.isEmpty());
//
//        final Class<? extends Channel>[] internalChannels = this.pickChannelClasses(
//                supportedOutputChannels, internalInputs, isRequestReusable);
//        if (internalChannels == null) {
//            return null;
//        }
//
//        return new Tuple<>(internalChannels, externalChannels);
//    }
//
//    private Class<? extends Channel>[] pickChannelClasses(List<Class<? extends Channel>> outputChannelClasses,
//                                                          List<InputSlot<Object>> inputs,
//                                                          boolean isRequestReusable) {
//        List<Class<? extends Channel>> permittedChannels = isRequestReusable ?
//                outputChannelClasses.stream()
//                        .filter(channelClass -> this.getChannelInitializer(channelClass).isReusable())
//                        .collect(Collectors.toList()) :
//                outputChannelClasses;
//        final List<List<Class<? extends Channel>>> inputChannelClassLists = inputs.stream()
//                .map(input -> ((ExecutionOperator) input.getOwner()).getSupportedInputChannels(input.getIndex()))
//                .collect(Collectors.toList());
//        final Class<? extends Channel>[] pickedChannels = this.pickChannelClasses(permittedChannels, inputChannelClassLists);
//        if (pickedChannels == null) {
//            return null;
//        }
//        return pickedChannels;
//    }
//
//    /**
//     * Designates a {@link Channel} class for each entry in {@code inputChannelClassLists}.
//     *
//     * @param outputChannelClasses   {@link Channel} classes that may be picked; ordered by preference
//     * @param inputChannelClassLists {@link List}s of {@link Channel}s; for each, one{@link Channel} should be picked
//     * @return an array containing the picked classes, aligned with {@code inputChannelClassLists}, or {@code null}
//     * if a full match was not possible
//     */
//    protected Class<? extends Channel>[] pickChannelClasses(List<Class<? extends Channel>> outputChannelClasses,
//                                                            List<List<Class<? extends Channel>>> inputChannelClassLists) {
//
//        // Keep track of the picked classes and how often it has been picked.
//        // NB: This greedy algorithm might be improved.
//        Class<? extends Channel>[] pickedChannelClasses = new Class[inputChannelClassLists.size()];
//        int[] maxMatches = new int[inputChannelClassLists.size()];
//        for (Class<? extends Channel> supportedOutputChannel : outputChannelClasses) {
//            boolean[] matches = new boolean[inputChannelClassLists.size()];
//            int numMatches = 0;
//            for (int i = 0; i < inputChannelClassLists.size(); i++) {
//                final List<Class<? extends Channel>> classes = inputChannelClassLists.get(i);
//                if ((matches[i] = classes.contains(supportedOutputChannel))) {
//                    numMatches++;
//                }
//            }
//            for (int i = 0; i < inputChannelClassLists.size(); i++) {
//                if (matches[i] && (pickedChannelClasses[i] == null || numMatches > maxMatches[i])) {
//                    pickedChannelClasses[i] = supportedOutputChannel;
//                    maxMatches[i] = numMatches;
//                }
//            }
//        }
//
//        for (Class<? extends Channel> pickedChannelClass : pickedChannelClasses) {
//            if (pickedChannelClass == null) return null;
//        }
//
//        return pickedChannelClasses;
//    }
//
//    protected boolean hasOnlySingleClass(Class[] array) {
//        for (int i = 1; i < array.length; i++) {
//            if (array[0] != array[i]) return false;
//        }
//        return true;
//    }


}
