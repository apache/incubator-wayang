package org.qcri.rheem.core.optimizer.cardinality;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.OperatorContainer;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.plan.rheemplan.PlanTraversal;
import org.qcri.rheem.core.util.OneTimeExecutable;
import org.qcri.rheem.core.util.RheemCollections;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Stream;

/**
 * {@link CardinalityEstimator} that subsumes a DAG of operators, each one providing a local {@link CardinalityEstimator}.
 */
public class CardinalityEstimationTraversal {

    private final Collection<Activation> inputActivations;

    private final Collection<? extends Activator> sourceActivators;

    /**
     * Create an instance that pushes {@link CardinalityEstimate}s through a data flow plan starting at the given
     * {@code inputSlots} and {@code sourceOperators}, thereby putting {@link CardinalityEstimate}s into the
     * {@code cache}.
     *
     * @param operatorContainer that should be traversed
     * @param configuration     provides utilities for the estimation
     */
    public static CardinalityEstimationTraversal createPushTraversal(OperatorContainer operatorContainer, Configuration configuration) {
        if (operatorContainer.isSource()) {
            return createPushTraversal(
                    Collections.emptyList(),
                    Collections.singleton(operatorContainer.getSource()),
                    configuration
            );
        } else {
            return createPushTraversal(
                    operatorContainer.getMappedInputs(),
                    Collections.emptyList(),
                    configuration
            );
        }
    }

    /**
     * Create an instance that pushes {@link CardinalityEstimate}s through a data flow plan starting at the given
     * {@code inputSlots} and {@code sourceOperators}, thereby putting {@link CardinalityEstimate}s into the
     * {@code cache}.
     *
     * @param inputSlots      open {@link InputSlot}s that will be initially activated
     * @param sourceOperators {@link Operator} that will be initially activated
     * @param configuration   provides utilties for the estimation
     */
    public static CardinalityEstimationTraversal createPushTraversal(Collection<InputSlot<?>> inputSlots,
                                                                     Collection<Operator> sourceOperators,
                                                                     Configuration configuration) {
        return createPushTraversal(inputSlots, Collections.emptySet(), sourceOperators, configuration);
    }


    /**
     * Create an instance that pushes {@link CardinalityEstimate}s through a data flow plan starting at the given
     * {@code inputSlots} and {@code sourceOperators}, thereby putting {@link CardinalityEstimate}s into the
     * {@code cache}.
     *
     * @param inputSlots       open {@link InputSlot}s that will be initially activated
     * @param borderInputSlots that will not be followed; they are terminal in addition to {@link OutputSlot}s that
     *                         have no occupied {@link InputSlot}s
     * @param sourceOperators  {@link Operator} that will be initially activated
     * @param configuration    provides utilties for the estimation
     */
    public static CardinalityEstimationTraversal createPushTraversal(Collection<InputSlot<?>> inputSlots,
                                                                     Collection<InputSlot<?>> borderInputSlots,
                                                                     Collection<Operator> sourceOperators,
                                                                     Configuration configuration) {
        Validate.notNull(inputSlots);
        Validate.notNull(sourceOperators);
        Validate.notNull(configuration);

        // Starting from the an output, find all required inputs.
        return new Builder(inputSlots, borderInputSlots, sourceOperators, configuration).build();
    }


    /**
     * Creates a new instance.
     *
     * @param inputActivations {@link Activation}s that will be satisfied by the parameters of
     *                         {@link CardinalityEstimator#estimate(Configuration, CardinalityEstimate...)} };
     *                         the indices of the {@link Activation}s match those
     *                         of the {@link CardinalityEstimate}s
     * @param sourceActivators {@link Activator}s of source {@link CardinalityEstimator}
     */
    private CardinalityEstimationTraversal(final Collection<Activation> inputActivations,
                                           Collection<? extends Activator> sourceActivators) {
        this.inputActivations = inputActivations;
        this.sourceActivators = sourceActivators;
    }

    /**
     * Traverse and update {@link CardinalityEstimate}s.
     *
     * @param optimizationContext provides input {@link CardinalityEstimate}s and stores all produces
     *                            {@link CardinalityEstimate}s alongside the push traversal
     * @param configuration       provides the applicable {@link Configuration}
     * @return whether any {@link CardinalityEstimate}s have been updated
     */
    public boolean traverse(OptimizationContext optimizationContext, Configuration configuration) {
        boolean isUpdated = false;
        try {
            final Queue<Activator> activators = this.initializeActivatorQueue();
            do {
                assert !activators.isEmpty() : String.format("No source activators. (input activations: %s)", this.inputActivations);
                final Activator activator = activators.poll();
                isUpdated |= activator.process(optimizationContext, configuration, activators);
            } while (!activators.isEmpty());
        } finally {
            this.reset();
        }
        return isUpdated;
    }

    /**
     * Set up a queue of initial {@link Activator}s for an estimation pass.
     */
    private Queue<Activator> initializeActivatorQueue() {
        Queue<Activator> activatedActivators = new LinkedList<>(this.sourceActivators);
        this.inputActivations.forEach(activation -> activation.fire(activatedActivators));
        return activatedActivators;
    }

    /**
     * Resets this instance, so that it perform a new traversal.
     */
    private void reset() {
        this.resetAll(Stream.concat(
                this.inputActivations.stream().map(Activation::getTargetActivator),
                this.sourceActivators.stream()
        ));
    }

    private void resetAll(Stream<Activator> activatorStream) {
        activatorStream
                .filter(Activator::reset)
                .flatMap(Activator::getAllDependentActivations)
                .map(Activation::getTargetActivator)
                .forEach(this::resetDownstream);
    }

    private void resetDownstream(Activator activator) {
        this.resetAll(Stream.of(activator));
    }

    /**
     * Wraps a {@link CardinalityEstimator}, thereby caching its input {@link CardinalityEstimate}s and keeping track
     * of its dependent {@link CardinalityEstimator}s.
     */
    private static class Activator {

        private final boolean[] isActivated;

        private final CardinalityPusher pusher;

        private final Collection<Activation>[] dependentActivations;

        /**
         * The {@link Operator} being wrapped by this instance.
         */
        private final Operator operator;

        @SuppressWarnings("unchecked")
        Activator(Operator operator, Configuration configuration) {
            this.operator = operator;
            this.isActivated = new boolean[operator.getNumInputs()];
            this.dependentActivations = new Collection[operator.getNumOutputs()];
            for (int outputIndex = 0; outputIndex < this.dependentActivations.length; outputIndex++) {
                this.dependentActivations[outputIndex] = new ArrayList<>(2);
            }
            this.pusher = operator.getCardinalityPusher(configuration);
        }

        /**
         * Execute this instance, thereby activating new instances and putting them on the queue.
         *
         * @param optimizationContext the current {@link OptimizationContext} in which the push should take place
         * @param activatorQueue      accepts newly activated {@link CardinalityEstimator}s
         */
        boolean process(OptimizationContext optimizationContext, Configuration configuration, Queue<Activator> activatorQueue) {
            OptimizationContext.OperatorContext opCtx = optimizationContext.getOperatorContext(this.operator);
            assert opCtx != null : String.format("Could not find OperatorContext for %s.", this.operator);

            // Do the local estimation.
            boolean isUpdated = this.pusher.push(opCtx, configuration);
            opCtx.pushCardinalitiesForward();

            for (int outputIndex = 0; outputIndex < this.operator.getNumOutputs(); outputIndex++) {
                // Trigger follow-up operators.
                this.processDependentActivations(this.dependentActivations[outputIndex], activatorQueue);
            }

            return isUpdated;
        }

        /**
         * Triggers the {@link #dependentActivations} and puts newly activated {@link Activator}s onto the
         * {@code activatorQueue}.
         */
        private void processDependentActivations(Collection<Activation> activations, Queue<Activator> activatorQueue) {
            // Otherwise, we update/activate the dependent estimators.
            activations.forEach(activation -> activation.fire(activatorQueue));
        }

        /**
         * Tells whether all input {@link Activation}s have fired.
         */
        boolean canBeActivated() {
            for (boolean isActivated : this.isActivated) {
                if (!isActivated) return false;
            }
            return true;
        }

        /**
         * Resets this instance.
         *
         * @return whether this instance was not already reset
         */
        boolean reset() {
            boolean isStateChanged = false;
            for (int inputIndex = 0; inputIndex < this.isActivated.length; inputIndex++) {
                isStateChanged |= this.isActivated[inputIndex];
                this.isActivated[inputIndex] = false;
            }
            return isStateChanged || this.isActivated.length == 0;
        }


        protected Activation createActivation(int inputIndex) {
            return new Activation(inputIndex, this);
        }


        protected Stream<Activation> getAllDependentActivations() {
            return Arrays.stream(this.dependentActivations).flatMap(Collection::stream);
        }

        protected Collection<Activation> getDependentActivations(OutputSlot<?> outputSlot) {
            return this.dependentActivations[outputSlot.getIndex()];
        }

        @Override
        public String toString() {
            StringBuilder activations = new StringBuilder(this.isActivated.length);
            for (boolean b : this.isActivated) {
                activations.append(b ? "|" : "-");
            }
            return String.format("%s[%s, %s]", this.getClass().getSimpleName(), this.operator, activations);
        }
    }


    /**
     * Describes a reference to an input of an {@link Activator}.
     */
    private static class Activation {

        /**
         * The input index on which the {@link #activator} will be activated.
         */
        final int inputIndex;

        /**
         * The {@link Activator} to be activated by this instance.
         */
        final Activator activator;

        public Activation(int inputIndex, Activator activator) {
            this.activator = activator;
            this.inputIndex = inputIndex;
        }

        public Activator getTargetActivator() {
            return this.activator;
        }

        public void fire(Queue<Activator> activatorQueue) {
            assert !this.activator.isActivated[this.inputIndex]
                    : String.format("%s is already activated at input %d.", this.activator.operator, this.inputIndex);
            this.activator.isActivated[this.inputIndex] = true;
            if (this.activator.canBeActivated()) {
                activatorQueue.add(this.activator);
            }
        }
    }

    /**
     * Utility to create a {@link CardinalityEstimationTraversal},
     */
    private static class Builder extends OneTimeExecutable {

        /**
         * {@link Configuration} that provides crucial information for the {@link CardinalityEstimate}s to be created.
         */
        final Configuration configuration;

        /**
         * {@link InputSlot}s that will provide initial {@link CardinalityEstimate}s.
         */
        final Collection<InputSlot<?>> inputSlots;

        /**
         * {@link InputSlot}s that will not be followed; they are terminal. Note that these are not necessarily
         * the only {@link OutputSlot}s in the created {@link CardinalityEstimationTraversal} that will not be
         * followed -- some {@link OutputSlot}s might not have occupied {@link InputSlot}s.
         */
        final Set<InputSlot<?>> borderInputSlots;

        /**
         * Source {@link Operator}s that should be part of the pushing.
         */
        private final Collection<Operator> sourceOperators;

        /**
         * Keeps {@link Activator}s around that have already been created.
         */
        private Map<Operator, Activator> createdActivators = new HashMap<>();

        /**
         * The finally build {@link CardinalityEstimationTraversal}.
         */
        private CardinalityEstimationTraversal result;

        /**
         * Creates a new instance.
         *
         * @param inputSlots       see {@link #inputSlots}
         * @param borderInputSlots see {@link #borderInputSlots}
         * @param sourceOperators  see {@link #sourceOperators}
         * @param configuration    see {@link #configuration}
         */
        private Builder(Collection<InputSlot<?>> inputSlots, Collection<InputSlot<?>> borderInputSlots, Collection<Operator> sourceOperators, Configuration configuration) {
            this.inputSlots = inputSlots;
            this.borderInputSlots = RheemCollections.asSet(borderInputSlots);
            this.configuration = configuration;
            this.sourceOperators = sourceOperators;
        }

        /**
         * Build the {@link CardinalityEstimationTraversal}.
         *
         * @return the build {@link CardinalityEstimationTraversal}
         */
        CardinalityEstimationTraversal build() {
            this.execute();
            return this.result;
        }

        /**
         * Builds an instance starting from {@link InputSlot}s and source {@link Operator}.
         */
        @Override
        public void doExecute() {
            Set<InputSlot<?>> distinctInputs = new HashSet<>(this.inputSlots);

            // Go through all relevant operators of and create EstimatorActivators.
            PlanTraversal.downstream()
                    .withCallback(this::addAndRegisterActivator)
                    .followingInputsDownstreamIf(input -> !this.borderInputSlots.contains(input))
                    .traverse(this.sourceOperators)
                    .traverse(distinctInputs.stream().map(InputSlot::getOwner));

            // Gather the required activations.
            final Collection<Activation> requiredActivations = new LinkedList<>();
            for (InputSlot<?> inputSlot : distinctInputs) {
                final Operator owner = inputSlot.getOwner();
                final Activator activator = this.createdActivators.get(owner);
                if (activator != null) {
                    requiredActivations.add(activator.createActivation(inputSlot.getIndex()));
                }
            }

            // Gather the source activators.
            Collection<Activator> sourceActivators = new LinkedList<>();
            for (Operator source : this.sourceOperators) {
                final Activator activator = this.createdActivators.get(source);
                if (activator != null) {
                    sourceActivators.add(activator);
                }
            }

            this.result = new CardinalityEstimationTraversal(requiredActivations, sourceActivators);
        }

        /**
         * If there is no registered {@link Activator} for the {@code operator} and/or no
         * {@link Activation}, then these will be created, registered, and connected.
         *
         * @param operator
         */
        private void addAndRegisterActivator(Operator operator) {
            // The operator should not have been processed yet.
            assert !this.createdActivators.containsKey(operator);

            // Otherwise, try to create the activator.
            Activator activator = this.createActivator(operator);

            // Register existing dependent activators.
            for (OutputSlot<?> outputSlot : operator.getAllOutputs()) {
                this.registerDependentActivations(outputSlot, activator);
            }

            // Register with required activators.
            this.registerAsDependentActivation(activator);

        }

        /**
         * @return the {@link Activator} that is associated to the owner of the {@code outputSlot}
         */
        protected Activator getCachedActivator(OutputSlot<?> outputSlot) {
            return this.createdActivators.get(outputSlot.getOwner());
        }

        /**
         * Create and register an {@link Activator} for the {@code operator}.
         *
         * @return the newly created {@link Activator}
         */
        protected Activator createActivator(Operator operator) {
            final Activator pusherActivator = new Activator(operator, this.configuration);
            this.createdActivators.put(operator, pusherActivator);
            return pusherActivator;
        }

        /**
         * Connect the {@code activator} with already existing {@link Activator}s that are fed by the {@code outputSlot}
         * via a new {@link Activation}.
         */
        protected void registerDependentActivations(OutputSlot<?> outputSlot, Activator activator) {
            for (InputSlot<?> inputSlot : outputSlot.getOccupiedSlots()) {
                Arrays.stream(inputSlot.getOwner().getAllOutputs())
                        .map(this::getCachedActivator)
                        .filter(Objects::nonNull)
                        .map(dependentActivator -> dependentActivator.createActivation(inputSlot.getIndex()))
                        .forEach(activator.getDependentActivations(outputSlot)::add);
            }
        }

        /**
         * Connect the {@code activator} with already existing {@link Activator}s that are fed by the {@code outputSlot}
         * via a new {@link Activation}.
         */
        protected void registerAsDependentActivation(Activator activator) {
            for (InputSlot<?> inputSlot : activator.operator.getAllInputs()) {
                final OutputSlot<?> occupant = inputSlot.getOccupant();
                if (Objects.isNull(occupant)) {
                    continue;
                }
                final Activator requiredActivator = this.getCachedActivator(occupant);
                if (requiredActivator == null) {
                    continue;
                }
                requiredActivator.getDependentActivations(occupant).add(activator.createActivation(inputSlot.getIndex()));
            }
        }
    }

}
