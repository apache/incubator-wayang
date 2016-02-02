package org.qcri.rheem.core.optimizer.costs;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.plan.*;
import org.qcri.rheem.core.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * {@link CardinalityEstimator} that subsumes a DAG of operators, each one providing a local {@link CardinalityEstimator}.
 */
public class CompositeCardinalityEstimator extends CardinalityEstimator.WithCache {

    private static final Logger LOGGER = LoggerFactory.getLogger(CompositeCardinalityEstimator.class);

    private final List<List<Activation>> inputActivations;

    private final Collection<Activator> sourceActivators;

    /**
     * Create an instance for the given {@link Subplan}.
     *
     * @return the instance if it could be created
     */
    public static Optional<CardinalityEstimator> createFor(Subplan subplan,
                                                           int outputIndex,
                                                           Map<OutputSlot<?>, CardinalityEstimate> cache) {
        final OutputSlot<?> subplanOutput = subplan.getOutput(outputIndex);
        final OutputSlot<?> innerOutput = subplan.traceOutput(subplanOutput);
        if (innerOutput == null) {
            return Optional.of(
                    new DefaultCardinalityEstimator(1d, subplan.getNumInputs(), inputCards -> 0L, subplanOutput, cache));
        }

        // Starting from the an output, find all required inputs.
        TopDownBuilder builder = new TopDownBuilder(cache);
        final Tuple<Map<InputSlot<?>, Collection<Activation>>, Collection<Activator>> builderOutput = builder.buildFor(innerOutput);
        if (builderOutput == null) {
            return Optional.empty();
        }
        final Map<InputSlot<?>, Collection<Activation>> requiredActivations = builderOutput.field0;
        final Collection<Activator> sourceActivators = builderOutput.field1;

        // Ensure that all required activations are connected to outer input slots.
        List<List<Activation>> inputActivations = new ArrayList<>(subplan.getNumInputs());
        for (InputSlot<?> outerInput : subplan.getAllInputs()) {
            final List<Activation> activationsForInput = subplan.followInput(outerInput).stream()
                    .flatMap(innerInput -> {
                        final Collection<Activation> activations = requiredActivations.remove(innerInput);
                        return activations == null ? Stream.empty() : activations.stream();
                    })
                    .collect(Collectors.toList());
            inputActivations.add(activationsForInput);
        }

        if (!requiredActivations.isEmpty()) {
            LOGGER.info("Could not build instance: unsatisfied required inputs");
            return Optional.empty();
        }

        return Optional.of(new CompositeCardinalityEstimator(inputActivations, sourceActivators, subplanOutput, cache));
    }

    /**
     * Creates a new instance.
     *
     * @param inputActivations {@link Activation}s that will be satisfied by the parameters of
     *                         {@link #estimate(RheemContext, CardinalityEstimate...)}; the indices of the {@link Activation}s match those
     *                         of the {@link CardinalityEstimate}s
     * @param sourceActivators {@link Activator}s of source {@link CardinalityEstimator}
     */
    private CompositeCardinalityEstimator(final List<List<Activation>> inputActivations,
                                          Collection<Activator> sourceActivators, final OutputSlot<?> targetOutputSlot,
                                          final Map<OutputSlot<?>, CardinalityEstimate> cache) {
        super(targetOutputSlot, cache);
        this.inputActivations = inputActivations;
        this.sourceActivators = sourceActivators;
    }

    @Override
    synchronized public CardinalityEstimate calculateEstimate(RheemContext rheemContext, CardinalityEstimate... inputEstimates) {
        final Queue<Activator> activators = initializeActivatorQueue(inputEstimates);
        Optional<CardinalityEstimate> optionalEstimate;
        do {
            final Activator activator = activators.poll();
            optionalEstimate = activator.process(rheemContext, activators);
            Validate.isTrue(optionalEstimate.isPresent() == activators.isEmpty());
        } while (!activators.isEmpty());
        reset();
        return optionalEstimate.get();
    }

    /**
     * Set up a queue of initial {@link Activator}s for an estimation pass.
     */
    private Queue<Activator> initializeActivatorQueue(CardinalityEstimate[] inputEstimates) {
        int inputIndex = 0;
        Queue<Activator> activatedActivators = new LinkedList<>(this.sourceActivators);
        for (List<Activation> activations : this.inputActivations) {
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
                .flatMap(List::stream)
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
    private static class Activator {

        final CardinalityEstimate[] inputEstimates;

        final CardinalityEstimator estimator;

        final Collection<Activation> dependentActivations = new LinkedList<>();

        Activator(CardinalityEstimator estimator, int numInputs) {
            this.inputEstimates = new CardinalityEstimate[numInputs];
            this.estimator = estimator;
        }

        boolean canBeActivated() {
            return Arrays.stream(this.inputEstimates).noneMatch(Objects::isNull);
        }

        /**
         * Call {@link CardinalityEstimator#estimate(RheemContext, CardinalityEstimate...)} on the wrapped
         * {@link CardinalityEstimator} and update/activate its dependent {@link CardinalityEstimator}s.
         *
         * @param rheemContext   necessary for {@link CardinalityEstimator#estimate(RheemContext, CardinalityEstimate...)}
         * @param activatorQueue accepts newly activated {@link CardinalityEstimator}s
         * @return optionally the {@link CardinalityEstimate} of this round if there is no dependent {@link CardinalityEstimator}
         */
        Optional<CardinalityEstimate> process(RheemContext rheemContext, Queue<Activator> activatorQueue) {
            // Do the local estimation.
            final CardinalityEstimate resultEstimate = this.estimator.estimate(rheemContext, this.inputEstimates);

            // If there is no dependent estimation to be done, the result must be the final result.
            if (this.dependentActivations.isEmpty()) {
                return Optional.of(resultEstimate);
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
            return Optional.empty();
        }
    }


    /**
     * Describes a reference to an input of an {@link Activator}.
     */
    private static class Activation {

        final int inputIndex;

        final Activator activator;

        public Activation(int inputIndex, Activator activator) {
            this.activator = activator;
            this.inputIndex = inputIndex;
        }
    }

    /**
     * Utility to create a {@link CompositeCardinalityEstimator},
     */
    private static class TopDownBuilder {

        private Map<OutputSlot<?>, Activator> estimatorActivators = new HashMap<>();

        private final Map<OutputSlot<?>, CardinalityEstimate> cache;

        private boolean isAllPartialEstimatesAvailable = true;

        private TopDownBuilder(Map<OutputSlot<?>, CardinalityEstimate> cache) {
            this.cache = cache;
        }

        public Tuple<Map<InputSlot<?>, Collection<Activation>>, Collection<Activator>> buildFor(OutputSlot<?> outputSlot) {
            // Go through all relevant operators of the subplan and create EstimatorActivators.
            new PlanTraversal(true, false).withCallback((operator, fromInputSlot, fromOutputSlot) -> {
                if (fromOutputSlot != null) {
                    this.add(fromOutputSlot);
                }
            }).traverse(outputSlot.getOwner(), null, outputSlot);

            if (!this.isAllPartialEstimatesAvailable) {
                LOGGER.info("Could not build instance: missing partial estimator");
                return null;
            }

            // Find all required activators.
            final Map<InputSlot<?>, Collection<Activation>> requiredActivations = new HashMap<>();
            final Collection<Activator> sourceActivators = new LinkedList<>();
            new PlanTraversal(true, false).withCallback((operator, fromInputSlot, fromOutputSlot) -> {
                final Activator activator = this.estimatorActivators.get(fromOutputSlot);
                if (operator.getNumInputs() == 0) {
                    sourceActivators.add(activator);
                }
                for (InputSlot<?> inputSlot : operator.getAllInputs()) {
                    if (inputSlot.getOccupant() != null) {
                        continue;
                    }
                    final Activation activation = new Activation(inputSlot.getIndex(), activator);
                    requiredActivations.compute(inputSlot, (inputSlot_, activations) -> {
                        if (activations == null) {
                            activations = new LinkedList<>();
                        }
                        activations.add(activation);
                        return activations;
                    });
                }
            }).traverse(outputSlot.getOwner(), null, outputSlot);

            return new Tuple<>(requiredActivations, sourceActivators);
        }

        private void add(OutputSlot<?> outputSlot) {
            // Get or create the estimator.
            final Activator activator = getOrCreateEstimatorActivator(outputSlot);
            if (activator == null) {
                this.isAllPartialEstimatesAvailable = false;
                return;
            }

            // Register existing dependent activators.
            registerDependentActivations(outputSlot, activator);

            // Register with required activators.
            registerAsDependentActivation(outputSlot, activator);

        }


        private Activator getOrCreateEstimatorActivator(OutputSlot<?> outputSlot) {
            // Try to serve the request from the cache.
            Activator activator = this.estimatorActivators.get(outputSlot);
            if (activator != null) {
                return activator;
            }

            // Otherwise, try to create the activator.
            final Operator operator = outputSlot.getOwner();
            final Optional<CardinalityEstimator> optionalEstimator =
                    operator.getCardinalityEstimator(outputSlot.getIndex(), this.cache);
            if (!optionalEstimator.isPresent()) {
                return null;
            }

            // On success, register and return it.
            activator = new Activator(optionalEstimator.get(), operator.getNumInputs());
            this.estimatorActivators.put(outputSlot, activator);
            return activator;
        }

        private void registerDependentActivations(OutputSlot<?> outputSlot, Activator activator) {
            for (InputSlot<?> inputSlot : outputSlot.getOccupiedSlots()) {
                Arrays.stream(inputSlot.getOwner().getAllOutputs())
                        .map(this.estimatorActivators::get)
                        .filter(Objects::nonNull)
                        .map(dependentActivator -> new Activation(inputSlot.getIndex(), dependentActivator))
                        .forEach(activator.dependentActivations::add);
            }
        }

        private void registerAsDependentActivation(OutputSlot<?> outputSlot, Activator activator) {
            for (InputSlot<?> inputSlot : outputSlot.getOwner().getAllInputs()) {
                final OutputSlot<?> occupant = inputSlot.getOccupant();
                if (Objects.isNull(occupant)) {
                    continue;
                }
                final Activator requiredActivator = this.estimatorActivators.get(occupant);
                if (requiredActivator == null) {
                    continue;
                }
                requiredActivator.dependentActivations.add(new Activation(inputSlot.getIndex(), activator));
            }
        }

    }

}
