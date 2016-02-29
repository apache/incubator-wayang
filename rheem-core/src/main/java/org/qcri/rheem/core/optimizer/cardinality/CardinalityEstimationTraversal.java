package org.qcri.rheem.core.optimizer.cardinality;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.plan.rheemplan.*;
import org.qcri.rheem.core.util.RheemCollections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * {@link CardinalityEstimator} that subsumes a DAG of operators, each one providing a local {@link CardinalityEstimator}.
 */
public class CardinalityEstimationTraversal {

    private static final Logger LOGGER = LoggerFactory.getLogger(CardinalityEstimationTraversal.class);

    private final List<Collection<Activation>> inputActivations;

    private final Collection<? extends Activator> sourceActivators;

    /**
     * Creates an instance towards the given {@link OutputSlot} starting at the upstream-most possible
     * {@link InputSlot}s and source.
     *
     * @return the instance if it could be created or else {@code null}
     * @deprecated Do we need this?
     */
    public static CardinalityEstimationTraversal createPullTraversal(List<Collection<InputSlot<?>>> inputSlots,
                                                                     OutputSlot<?> targetOutput,
                                                                     Configuration configuration) {
        Validate.notNull(inputSlots);
        Validate.notNull(targetOutput);
        Validate.notNull(configuration);

        // Starting from the an output, find all required inputs.
        return new EstimatorBuilder(inputSlots, configuration).build(targetOutput);
    }

    /**
     * Create an instance that pushes {@link CardinalityEstimate}s through a data flow plan starting at the given
     * {@code inputSlots} and {@code sourceOperators}, thereby putting {@link CardinalityEstimate}s into the
     * {@code cache}.
     *
     * @param inputSlots a multi-{@link List} of {@link InputSlot}s where the instance should begin to traverse from;
     *                   the method {@link #traverse(Configuration, CardinalityEstimate...)} will expect exactly one
     *                   {@link CardinalityEstimate} for each entry in this multi-{@link List} and each of the
     *                   {@link CardinalityEstimate}s will be delivered to all {@link InputSlot}s according to their
     *                   order of appearance
     */
    public static CardinalityEstimationTraversal createPushTraversal(List<Collection<InputSlot<?>>> inputSlots,
                                                                     Collection<Operator> sourceOperators,
                                                                     Configuration configuration) {
        Validate.notNull(inputSlots);
        Validate.notNull(sourceOperators);
        Validate.notNull(configuration);

        // Starting from the an output, find all required inputs.
        return new PusherBuilder(inputSlots, configuration).build(sourceOperators);
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
    private CardinalityEstimationTraversal(final List<Collection<Activation>> inputActivations,
                                           Collection<? extends Activator> sourceActivators) {
        this.inputActivations = inputActivations;
        this.sourceActivators = sourceActivators;
    }


    public Map<OutputSlot<?>, CardinalityEstimate> traverse(Configuration configuration, CardinalityEstimate... inputEstimates) {
        final Queue<Activator> activators = this.initializeActivatorQueue(inputEstimates);
        final Map<OutputSlot<?>, CardinalityEstimate> terminalEstimates = new HashMap<>();
        do {
            assert !activators.isEmpty() : String.format("No source activators. (input activations: %s)", this.inputActivations);
            final Activator activator = activators.poll();
            activator.process(configuration, activators, terminalEstimates);
        } while (!activators.isEmpty());
        this.reset();
        return terminalEstimates;
    }

    /**
     * Set up a queue of initial {@link Activator}s for an estimation pass.
     */
    private Queue<Activator> initializeActivatorQueue(CardinalityEstimate[] inputEstimates) {
        int inputIndex = 0;
        Queue<Activator> activatedActivators = new LinkedList<>(this.sourceActivators);
        for (Collection<Activation> activations : this.inputActivations) {
            for (Activation activation : activations) {
                final CardinalityEstimate inputEstimate = inputEstimates[inputIndex];
                final Activator activator = activation.activator;
                activator.inputEstimates[activation.inputIndex] = inputEstimate;
                if (activator.canBeActivated()) {
                    activatedActivators.add(activator);
                }
            }
            inputIndex++;
        }
        return activatedActivators;
    }

    private void reset() {
        this.inputActivations.stream()
                .flatMap(Collection::stream)
                .forEach(this::reset);
        this.sourceActivators.stream()
                .flatMap(Activator::getAllDependentActivations)
                .forEach(this::reset);
    }

    private void reset(Activation activation) {
        final Activator activator = activation.activator;
        Arrays.fill(activator.inputEstimates, null);
        activator.getAllDependentActivations().forEach(this::reset);
    }

    /**
     * Wraps a {@link CardinalityEstimator}, thereby caching its input {@link CardinalityEstimate}s and keeping track
     * of its dependent {@link CardinalityEstimator}s.
     */
    static abstract class Activator {

        protected final CardinalityEstimate[] inputEstimates;

        protected Activator(int numInputs) {
            this.inputEstimates = new CardinalityEstimate[numInputs];
        }

        protected boolean canBeActivated() {
            return Arrays.stream(this.inputEstimates).noneMatch(Objects::isNull);
        }

        /**
         * Execute this instance, thereby activating new instances and putting them on the queue.
         *
         * @param activatorQueue    accepts newly activated {@link CardinalityEstimator}s
         * @param terminalEstimates @return optionally the {@link CardinalityEstimate} of this round if there is no dependent {@link CardinalityEstimator}
         */
        protected abstract void process(Configuration configuration,
                                        Queue<Activator> activatorQueue,
                                        Map<OutputSlot<?>, CardinalityEstimate> terminalEstimates);

        protected void processDependentActivations(OutputSlot<?> outputSlot,
                                                   CardinalityEstimate estimate,
                                                   Collection<Activation> activations,
                                                   Queue<Activator> activatorQueue,
                                                   Map<OutputSlot<?>, CardinalityEstimate> terminalEstimates) {
            // If there is no dependent estimation to be done, the result must be the final result.
            if (activations.isEmpty()) {
                terminalEstimates.put(outputSlot, estimate);
            }

            // Otherwise, we update/activate the dependent estimators.
            for (Activation dependentActivatorDescriptor : activations) {
                final int inputIndex = dependentActivatorDescriptor.inputIndex;
                final Activator activator = dependentActivatorDescriptor.activator;
                //Validate.isTrue(activator.inputEstimates[inputIndex] == null);
                activator.inputEstimates[inputIndex] = estimate;
                if (activator.canBeActivated()) {
                    activatorQueue.add(activator);
                }
            }
        }

        protected Activation createActivation(int inputIndex) {
            return new Activation(inputIndex, this);
        }

        protected abstract Stream<Activation> getAllDependentActivations();

        protected abstract Collection<Activation> getDependentActivations(OutputSlot<?> outputSlot);
    }

    /**
     * Wraps a {@link CardinalityEstimator}, thereby caching its input {@link CardinalityEstimate}s and keeping track
     * of its dependent {@link CardinalityEstimator}s.
     */
    static class EstimatorActivator extends Activator {

        private final CardinalityEstimator estimator;

        private final Collection<Activation> dependentActivations = new LinkedList<>();

        private final OutputSlot<?> estimatorOutput;

        EstimatorActivator(CardinalityEstimator estimator, int numInputs, OutputSlot<?> estimatorOutput) {
            super(numInputs);
            this.estimator = estimator;
            this.estimatorOutput = estimatorOutput;
        }

        @Override
        protected void process(Configuration configuration,
                               Queue<Activator> activatorQueue,
                               Map<OutputSlot<?>, CardinalityEstimate> terminalEstimates) {

            // Do the local estimation.
            final CardinalityEstimate resultEstimate = this.estimator.estimate(configuration, this.inputEstimates);
            this.processDependentActivations(this.estimatorOutput, resultEstimate,
                    this.dependentActivations, activatorQueue, terminalEstimates);
        }

        @Override
        protected Stream<Activation> getAllDependentActivations() {
            return this.dependentActivations.stream();
        }

        @Override
        protected Collection<Activation> getDependentActivations(OutputSlot<?> outputSlot) {
            return this.dependentActivations;
        }
    }

    /**
     * Wraps a {@link CardinalityEstimator}, thereby caching its input {@link CardinalityEstimate}s and keeping track
     * of its dependent {@link CardinalityEstimator}s.
     */
    static class PusherActivator extends Activator {

        private final CardinalityPusher pusher;

        @SuppressWarnings("unchecked")
        private final Collection<Activation>[] dependentActivations;

        PusherActivator(Operator operator,
                        Configuration configuration) {
            super(operator.getNumInputs());
            this.dependentActivations = new Collection[operator.getNumOutputs()];
            for (int outputIndex = 0; outputIndex < this.dependentActivations.length; outputIndex++) {
                this.dependentActivations[outputIndex] = new LinkedList<>();

            }
            this.pusher = operator.getCardinalityPusher(configuration);
        }

        @Override
        protected void process(Configuration configuration,
                               Queue<Activator> activatorQueue,
                               Map<OutputSlot<?>, CardinalityEstimate> terminalEstimates) {

            // Do the local estimation.
            this.pusher.push(configuration);

            for (int outputIndex = 0; outputIndex < this.pusher.getOperator().getNumOutputs(); outputIndex++) {
                // Trigger follow-up operators.
                this.processDependentActivations(this.pusher.getOperator().getOutput(outputIndex),
                        this.pusher.getOperator().getOutput(outputIndex).getCardinalityEstimate(),
                        this.dependentActivations[outputIndex],
                        activatorQueue,
                        terminalEstimates);
            }
        }

        @Override
        protected Stream<Activation> getAllDependentActivations() {
            return Arrays.stream(this.dependentActivations).flatMap(Collection::stream);
        }

        @Override
        protected Collection<Activation> getDependentActivations(OutputSlot<?> outputSlot) {
            return this.dependentActivations[outputSlot.getIndex()];
        }
    }


    /**
     * Describes a reference to an input of an {@link Activator}.
     */
    static class Activation {

        final int inputIndex;

        final Activator activator;

        public Activation(int inputIndex, Activator activator) {
            this.activator = activator;
            this.inputIndex = inputIndex;
        }
    }

    /**
     * Utility to create a {@link CardinalityEstimationTraversal},
     */
    static abstract class Builder {

        final List<Collection<InputSlot<?>>> inputSlots;

        final Configuration configuration;

        protected boolean isAllPartialEstimatorsAvailable = true;

        private Builder(List<Collection<InputSlot<?>>> inputSlots,
                        Configuration configuration) {
            this.inputSlots = inputSlots;
            this.configuration = configuration;
        }

        /**
         * Register an {@link Activation}.
         *
         * @param collector the register that the activation should be added to
         * @param activator an {@link Activator} whose {@link Activation} should be registered
         * @param inputSlot the {@link InputSlot} for which the {@link Activation} holds; also the key in the index
         */
        protected void addActivation(Map<InputSlot<?>, Collection<Activation>> collector, Activator activator, InputSlot<?> inputSlot) {
            final Activation activation = activator.createActivation(inputSlot.getIndex());
            RheemCollections.put(collector, inputSlot, activation);
        }

        protected abstract Activator getCachedActivator(OutputSlot<?> outputSlot);

        protected void registerDependentActivations(OutputSlot<?> outputSlot, Activator activator) {
            for (InputSlot<?> inputSlot : outputSlot.getOccupiedSlots()) {
                Arrays.stream(inputSlot.getOwner().getAllOutputs())
                        .map(this::getCachedActivator)
                        .filter(Objects::nonNull)
                        .map(dependentActivator -> dependentActivator.createActivation(inputSlot.getIndex()))
                        .forEach(activator.getDependentActivations(outputSlot)::add);
            }
        }

        protected void registerAsDependentActivation(OutputSlot<?> outputSlot, Activator activator) {
            for (InputSlot<?> inputSlot : outputSlot.getOwner().getAllInputs()) {
                final OutputSlot<?> occupant = inputSlot.getOccupant();
                if (Objects.isNull(occupant)) {
                    continue;
                }
                final Activator requiredActivator = this.getCachedActivator(occupant);
                if (requiredActivator == null) {
                    continue;
                }
                requiredActivator.getDependentActivations(outputSlot).add(activator.createActivation(inputSlot.getIndex()));
            }
        }

        /**
         * Translate the given {@link InputSlot}s to {@link Activation}s.
         *
         * @param inputSlotMultiList a multi-{@link List} of {@link InputSlot}s
         * @param inputActivations   a dictionary to map {@link }
         * @return
         */
        protected List<Collection<Activation>> alignActivations(final List<Collection<InputSlot<?>>> inputSlotMultiList,
                                                                final Map<InputSlot<?>, Collection<Activation>> inputActivations) {
            // Ensure that all required activations are connected to outer input slots.
            List<Collection<Activation>> alignedActivations = new ArrayList<>(inputSlotMultiList.size());
            for (Collection<InputSlot<?>> inputSlots : inputSlotMultiList) {
                final List<Activation> activationList = inputSlots.stream()
                        .flatMap(input -> {
                            final Collection<Activation> activations = inputActivations.remove(input);
                            return activations == null ? Stream.empty() : activations.stream();
                        })
                        .collect(Collectors.toList());
                alignedActivations.add(activationList);
            }

            return alignedActivations;
        }

    }

    /**
     * Utility to create a {@link CardinalityEstimationTraversal},
     */
    static class EstimatorBuilder extends CardinalityEstimationTraversal.Builder {

        private Map<OutputSlot<?>, EstimatorActivator> createdEstimatorActivators = new HashMap<>();

        private EstimatorBuilder(List<Collection<InputSlot<?>>> inputSlots, Configuration configuration) {
            super(inputSlots, configuration);
        }

        /**
         * Build an instance towards an {@link OutputSlot}.
         *
         * @deprecated Does anybody use this method? It's not suitable for cardinality pushing.
         */
        public CardinalityEstimationTraversal build(OutputSlot<?> outputSlot) {
            // Go through all relevant operators of the subplan and create EstimatorActivators.
            new PlanTraversal(true, false).withCallback((operator, fromInputSlot, fromOutputSlot) -> {
                if (fromOutputSlot != null) {
                    this.addAndRegisterActivator(fromOutputSlot);
                }
            }).traverse(outputSlot.getOwner(), null, outputSlot);

            if (!this.isAllPartialEstimatorsAvailable) {
                LOGGER.debug("Could not build instance: missing partial estimator");
                return null;
            }

            // Find all required activators.
            final Map<InputSlot<?>, Collection<Activation>> requiredActivations = new HashMap<>();
            final Collection<EstimatorActivator> sourceActivators = new LinkedList<>();
            new PlanTraversal(true, false).withCallback((operator, fromInputSlot, fromOutputSlot) -> {
                final EstimatorActivator activator = this.createdEstimatorActivators.get(fromOutputSlot);
                if (operator.getNumInputs() == 0) {
                    sourceActivators.add(activator);
                }
                for (InputSlot<?> inputSlot : operator.getAllInputs()) {
                    if (inputSlot.getOccupant() != null) {
                        continue;
                    }
                    this.addActivation(requiredActivations, activator, inputSlot);
                }
            }).traverse(outputSlot.getOwner(), null, outputSlot);

            final List<Collection<Activation>> alignedActivations = this.alignActivations(this.inputSlots, requiredActivations);
            if (!requiredActivations.isEmpty()) {
                LOGGER.warn("Could not build instance: unsatisfied required inputs");
                return null;
            }

            return new CardinalityEstimationTraversal(alignedActivations, sourceActivators);
        }

        protected void addAndRegisterActivator(OutputSlot<?> outputSlot) {
            // See if the output slot has already been processed.
            Activator activator = this.getCachedActivator(outputSlot);
            if (activator != null) {
                return;
            }

            activator = this.createAndCacheActivator(outputSlot);
            if (activator == null) {
                this.isAllPartialEstimatorsAvailable = false;
                return;
            }

            // Register existing dependent activators.
            this.registerDependentActivations(outputSlot, activator);

            // Register with required activators.
            this.registerAsDependentActivation(outputSlot, activator);

        }


        @Override
        protected Activator getCachedActivator(OutputSlot<?> outputSlot) {
            return this.createdEstimatorActivators.get(outputSlot);
        }

        protected EstimatorActivator createAndCacheActivator(OutputSlot<?> outputSlot) {
            final Operator operator = outputSlot.getOwner();
            final Optional<CardinalityEstimator> optionalEstimator =
                    operator.getCardinalityEstimator(outputSlot.getIndex(), this.configuration);
            if (!optionalEstimator.isPresent()) {
                return null;
            }
            final EstimatorActivator activator = new EstimatorActivator(optionalEstimator.get(),
                    operator.getNumInputs(),
                    outputSlot);
            this.createdEstimatorActivators.put(outputSlot, activator);
            return activator;
        }
    }

    /**
     * Utility to create a {@link CardinalityEstimationTraversal},
     */
    static class PusherBuilder extends CardinalityEstimationTraversal.Builder {

        private Map<Operator, PusherActivator> createdPusherActivators = new HashMap<>();

        private PusherBuilder(List<Collection<InputSlot<?>>> inputSlots,
                              Configuration configuration) {
            super(inputSlots, configuration);
        }

        /**
         * Builds an instance starting from {@link InputSlot}s and source {@link Operator}.
         */
        public CardinalityEstimationTraversal build(Collection<Operator> sources) {
            Set<InputSlot<?>> distinctInputs = this.inputSlots.stream()
                    .flatMap(Collection::stream)
                    .collect(Collectors.toSet());

            // Go through all relevant operators of the subplan and create EstimatorActivators.
            new PlanTraversal(false, true).withCallback((operator, fromInputSlot, fromOutputSlot) -> {
                for (OutputSlot<?> outputSlot : operator.getAllOutputs()) {
                    this.addAndRegisterActivator(outputSlot);
                }
            }).traverse(sources)
                    .traverse(distinctInputs.stream().map(InputSlot::getOwner).collect(Collectors.toList()));

            if (!this.isAllPartialEstimatorsAvailable) {
                LOGGER.debug("Could not build instance: missing partial estimator");
                return null;
            }

            // Gather the required activations.
            final Map<InputSlot<?>, Collection<Activation>> requiredActivations = new HashMap<>();
            for (InputSlot<?> inputSlot : distinctInputs) {
                final Operator owner = inputSlot.getOwner();
                final Activator activator = this.createdPusherActivators.get(owner);
                if (activator != null) {
                    this.addActivation(requiredActivations, activator, inputSlot);
                }
            }

            // Gather the source activators.
            Collection<Activator> sourceActivators = new LinkedList<>();
            for (Operator source : sources) {
                final Activator activator = this.createdPusherActivators.get(source);
                if (activator != null) {
                    sourceActivators.add(activator);
                }
            }

            final List<Collection<Activation>> alignedActivations = this.alignActivations(this.inputSlots, requiredActivations);
            if (!requiredActivations.isEmpty()) {
                LOGGER.warn("Could not build instance: unsatisfied required inputs");
                return null;
            }

            return new CardinalityEstimationTraversal(alignedActivations, sourceActivators);
        }

        private void addAndRegisterActivator(OutputSlot<?> outputSlot) {
            // See if the output slot has already been processed.
            Activator activator = this.getCachedActivator(outputSlot);
            if (activator != null) {
                return;
            }

            // Otherwise, try to create the activator.
            activator = this.createActivator(outputSlot);

            // Register existing dependent activators.
            this.registerDependentActivations(outputSlot, activator);

            // Register with required activators.
            this.registerAsDependentActivation(outputSlot, activator);

        }

        @Override
        protected Activator getCachedActivator(OutputSlot<?> outputSlot) {
            return this.createdPusherActivators.get(outputSlot.getOwner());
        }

        protected Activator createActivator(OutputSlot<?> outputSlot) {
            final Operator operator = outputSlot.getOwner();
            final PusherActivator pusherActivator = new PusherActivator(operator, this.configuration);
            this.createdPusherActivators.put(operator, pusherActivator);
            return pusherActivator;
        }


    }

}
