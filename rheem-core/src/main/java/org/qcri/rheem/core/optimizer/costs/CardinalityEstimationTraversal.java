package org.qcri.rheem.core.optimizer.costs;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.plan.InputSlot;
import org.qcri.rheem.core.plan.Operator;
import org.qcri.rheem.core.plan.OutputSlot;
import org.qcri.rheem.core.plan.PlanTraversal;
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

    private final Collection<Activator> sourceActivators;

    private final Map<OutputSlot<?>, CardinalityEstimate> cache;

    /**
     * Creates an instance towards the given {@link OutputSlot} starting at the upstream-most possible
     * {@link InputSlot}s and source.
     *
     * @return the instance if it could be created or else {@code null}
     */
    public static CardinalityEstimationTraversal createTopDown(List<Collection<InputSlot<?>>> inputSlots,
                                                               OutputSlot<?> targetOutput,
                                                               Map<OutputSlot<?>, CardinalityEstimate> cache) {
        Validate.notNull(targetOutput);

        // Starting from the an output, find all required inputs.
        return new Builder(inputSlots, cache).buildDownstream(targetOutput);
    }


    /**
     * Creates a new instance.
     *
     * @param inputActivations            {@link Activation}s that will be satisfied by the parameters of
     *                                    {@link CardinalityEstimator#estimate(RheemContext, CardinalityEstimate...)} };
     *                                    the indices of the {@link Activation}s match those
     *                                    of the {@link CardinalityEstimate}s
     * @param sourceActivators            {@link Activator}s of source {@link CardinalityEstimator}
     * @param isCreateActivationsOnTheFly on the first call of {@link #traverse(RheemContext, CardinalityEstimate...)}
     *                                    build up the graph of {@link Activation}s
     * @param cache
     */
    private CardinalityEstimationTraversal(final List<Collection<Activation>> inputActivations,
                                           Collection<Activator> sourceActivators,
                                           boolean isCreateActivationsOnTheFly,
                                           Map<OutputSlot<?>, CardinalityEstimate> cache) {
        this.inputActivations = inputActivations;
        this.sourceActivators = sourceActivators;
        this.cache = cache;
    }


    public Map<OutputSlot<?>, CardinalityEstimate> traverse(RheemContext rheemContext, CardinalityEstimate... inputEstimates) {
        final Queue<Activator> activators = initializeActivatorQueue(inputEstimates);
        final Map<OutputSlot<?>, CardinalityEstimate> terminalEstimates = new HashMap<>();
        do {
            final Activator activator = activators.poll();
            activator.process(rheemContext, activators, terminalEstimates);
        } while (!activators.isEmpty());
        reset();
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
                .flatMap(activator -> activator.dependentActivations.stream())
                .forEach(this::reset);
    }

    private void reset(Activation activation) {
        final Activator activator = activation.activator;
        Arrays.fill(activator.inputEstimates, null);
        for (Activation dependentActivation : activator.dependentActivations) {
            reset(dependentActivation);
        }
    }

    /**
     * Wraps a {@link CardinalityEstimator}, thereby caching its input {@link CardinalityEstimate}s and keeping track
     * of its dependent {@link CardinalityEstimator}s.
     */
    static class Activator {

        private final CardinalityEstimate[] inputEstimates;

        private final CardinalityEstimator estimator;

        private final Collection<Activation> dependentActivations = new LinkedList<>();

        Activator(CardinalityEstimator estimator, int numInputs) {
            this.inputEstimates = new CardinalityEstimate[numInputs];
            this.estimator = estimator;
        }

        private boolean canBeActivated() {
            return Arrays.stream(this.inputEstimates).noneMatch(Objects::isNull);
        }

        /**
         * Call {@link CardinalityEstimator#estimate(RheemContext, CardinalityEstimate...)} on the wrapped
         * {@link CardinalityEstimator} and update/activate its dependent {@link CardinalityEstimator}s.
         *
         * @param rheemContext      necessary for {@link CardinalityEstimator#estimate(RheemContext, CardinalityEstimate...)}
         * @param activatorQueue    accepts newly activated {@link CardinalityEstimator}s
         * @param terminalEstimates
         * @return optionally the {@link CardinalityEstimate} of this round if there is no dependent {@link CardinalityEstimator}
         */
        private void process(RheemContext rheemContext,
                             Queue<Activator> activatorQueue,
                             Map<OutputSlot<?>, CardinalityEstimate> terminalEstimates) {
            // Do the local estimation.
            final CardinalityEstimate resultEstimate = this.estimator.estimate(rheemContext, this.inputEstimates);

            // If there is no dependent estimation to be done, the result must be the final result.
            if (this.dependentActivations.isEmpty()) {
                terminalEstimates.put(this.estimator.getTargetOutput(), resultEstimate);
            }

            // Otherwise, we update/activate the dependent estimators.
            for (Activation dependentActivatorDescriptor : this.dependentActivations) {
                final int inputIndex = dependentActivatorDescriptor.inputIndex;
                final Activator activator = dependentActivatorDescriptor.activator;
                Validate.isTrue(activator.inputEstimates[inputIndex] == null);
                activator.inputEstimates[inputIndex] = resultEstimate;
                if (activator.canBeActivated()) {
                    activatorQueue.add(activator);
                }
            }
        }

        private Activation createActivation(int inputIndex) {
            return new Activation(inputIndex, this);
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
    static class Builder {

        final List<Collection<InputSlot<?>>> inputSlots;

        private final Map<OutputSlot<?>, CardinalityEstimate> cache;

        private boolean isAllPartialEstimatorsAvailable = true;

        private Map<OutputSlot<?>, Activator> createdActivators = new HashMap<>();

        private Builder(List<Collection<InputSlot<?>>> inputSlots, Map<OutputSlot<?>, CardinalityEstimate> cache) {
            this.inputSlots = inputSlots;
            this.cache = cache;
        }

        /**
         * Build an instance towards an {@link OutputSlot}.
         */
        public CardinalityEstimationTraversal buildDownstream(OutputSlot<?> outputSlot) {
            // Go through all relevant operators of the subplan and create EstimatorActivators.
            new PlanTraversal(true, false).withCallback((operator, fromInputSlot, fromOutputSlot) -> {
                if (fromOutputSlot != null) {
                    this.addAndRegisterActivator(fromOutputSlot);
                }
            }).traverse(outputSlot.getOwner(), null, outputSlot);

            if (!this.isAllPartialEstimatorsAvailable) {
                LOGGER.info("Could not build instance: missing partial estimator");
                return null;
            }

            // Find all required activators.
            final Map<InputSlot<?>, Collection<Activation>> requiredActivations = new HashMap<>();
            final Collection<Activator> sourceActivators = new LinkedList<>();
            new PlanTraversal(true, false).withCallback((operator, fromInputSlot, fromOutputSlot) -> {
                final Activator activator = this.createdActivators.get(fromOutputSlot);
                if (operator.getNumInputs() == 0) {
                    sourceActivators.add(activator);
                }
                for (InputSlot<?> inputSlot : operator.getAllInputs()) {
                    if (inputSlot.getOccupant() != null) {
                        continue;
                    }
                    addActivation(requiredActivations, activator, inputSlot);
                }
            }).traverse(outputSlot.getOwner(), null, outputSlot);

            final List<Collection<Activation>> alignedActivations = alignActivations(this.inputSlots, requiredActivations);
            if (!requiredActivations.isEmpty()) {
                LOGGER.warn("Could not build instance: unsatisfied required inputs");
                return null;
            }

            return new CardinalityEstimationTraversal(alignedActivations, sourceActivators, false, this.cache);
        }

        private void addActivation(Map<InputSlot<?>, Collection<Activation>> collector, Activator activator, InputSlot<?> inputSlot) {
            final Activation activation = activator.createActivation(inputSlot.getIndex());
            collector.compute(inputSlot, (inputSlot_, activations) -> {
                if (activations == null) {
                    activations = new LinkedList<>();
                }
                activations.add(activation);
                return activations;
            });
        }


        /**
         * Builds an instance starting from {@link InputSlot}s and source {@link Operator}.
         */
        public CardinalityEstimationTraversal buildUpstream(List<InputSlot<?>> inputSlots, List<Operator> sources) {

            // Go through all relevant operators of the subplan and create EstimatorActivators.
            new PlanTraversal(false, true).withCallback((operator, fromInputSlot, fromOutputSlot) -> {
                for (OutputSlot<?> outputSlot : operator.getAllOutputs()) {
                    this.addAndRegisterActivator(outputSlot);
                }
            }).traverse(sources)
                    .traverse(inputSlots.stream().map(InputSlot::getOwner).collect(Collectors.toList()));

            if (!this.isAllPartialEstimatorsAvailable) {
                LOGGER.info("Could not build instance: missing partial estimator");
                return null;
            }

            // Gather the required activations.
            final Map<InputSlot<?>, Collection<Activation>> requiredActivations = new HashMap<>();
            for (InputSlot<?> inputSlot : inputSlots) {
                final Operator owner = inputSlot.getOwner();
                for (OutputSlot<?> outputSlot : owner.getAllOutputs()) {
                    final Activator activator = this.createdActivators.get(outputSlot);
                    if (activator != null) {
                        addActivation(requiredActivations, activator, inputSlot);
                    }
                }
            }

            // Gather the source activators.
            Collection<Activator> sourceActivators = new LinkedList<>();
            for (Operator source : sources) {
                for (OutputSlot<?> outputSlot : source.getAllOutputs()) {
                    final Activator activator = this.createdActivators.get(outputSlot);
                    if (activator != null) {
                        sourceActivators.add(activator);
                    }
                }
            }

            final List<Collection<Activation>> alignedActivations = alignActivations(this.inputSlots, requiredActivations);
            if (!requiredActivations.isEmpty()) {
                LOGGER.warn("Could not build instance: unsatisfied required inputs");
                return null;
            }

            return new CardinalityEstimationTraversal(alignedActivations, sourceActivators, false, this.cache);
        }

        private void addAndRegisterActivator(OutputSlot<?> outputSlot) {
            // See if the output slot has already been processed.
            Activator activator = this.createdActivators.get(outputSlot);
            if (activator != null) {
                return;
            }

            // Otherwise, try to create the activator.
            final Operator operator = outputSlot.getOwner();
            final Optional<CardinalityEstimator> optionalEstimator =
                    operator.getCardinalityEstimator(outputSlot.getIndex(), this.cache);
            if (!optionalEstimator.isPresent()) {
                return;
            }
            activator = new Activator(optionalEstimator.get(), operator.getNumInputs());

            // On success, register and return it.
            this.createdActivators.put(outputSlot, activator);
            if (activator == null) {
                this.isAllPartialEstimatorsAvailable = false;
                return;
            }

            // Register existing dependent activators.
            registerDependentActivations(outputSlot, activator);

            // Register with required activators.
            registerAsDependentActivation(outputSlot, activator);

        }

        private void registerDependentActivations(OutputSlot<?> outputSlot, Activator activator) {
            for (InputSlot<?> inputSlot : outputSlot.getOccupiedSlots()) {
                Arrays.stream(inputSlot.getOwner().getAllOutputs())
                        .map(this.createdActivators::get)
                        .filter(Objects::nonNull)
                        .map(dependentActivator -> dependentActivator.createActivation(inputSlot.getIndex()))
                        .forEach(activator.dependentActivations::add);
            }
        }

        private void registerAsDependentActivation(OutputSlot<?> outputSlot, Activator activator) {
            for (InputSlot<?> inputSlot : outputSlot.getOwner().getAllInputs()) {
                final OutputSlot<?> occupant = inputSlot.getOccupant();
                if (Objects.isNull(occupant)) {
                    continue;
                }
                final Activator requiredActivator = this.createdActivators.get(occupant);
                if (requiredActivator == null) {
                    continue;
                }
                requiredActivator.dependentActivations.add(activator.createActivation(inputSlot.getIndex()));
            }
        }

        private List<Collection<Activation>> alignActivations(final List<Collection<InputSlot<?>>> inputSlotMultiList,
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

}
