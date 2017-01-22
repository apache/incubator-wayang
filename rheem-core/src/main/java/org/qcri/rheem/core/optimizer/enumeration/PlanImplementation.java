package org.qcri.rheem.core.optimizer.enumeration;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.ProbabilisticDoubleInterval;
import org.qcri.rheem.core.optimizer.costs.TimeEstimate;
import org.qcri.rheem.core.optimizer.costs.TimeToCostConverter;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.rheemplan.*;
import org.qcri.rheem.core.platform.Junction;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.util.Canonicalizer;
import org.qcri.rheem.core.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Represents a partial execution plan.
 */
public class PlanImplementation {

    private static final Logger logger = LoggerFactory.getLogger(PlanImplementation.class);

    /**
     * {@link ExecutionOperator}s contained in this instance.
     */
    private final Canonicalizer<ExecutionOperator> operators;

    /**
     * Describes the {@link Channel}s that have been picked between {@link ExecutionOperator}s and how they are
     * implemented.
     */
    private final Map<OutputSlot<?>, Junction> junctions;

    /**
     * Defines how {@link LoopSubplan}s should be executed.
     */
    private final Map<LoopSubplan, LoopImplementation> loopImplementations = new HashMap<>();

    /**
     * An enumerated plan is mainly characterized by the {@link OperatorAlternative.Alternative}s that have
     * been picked so far. This member keeps track of them.
     */
    private final Map<OperatorAlternative, OperatorAlternative.Alternative> settledAlternatives =
            new HashMap<>();

    /**
     * The {@link PlanEnumeration} that hosts this instance. Can change over time.
     */
    // TODO: I think, we don't maintain this field properly. Also, its semantics blur inside of LoopImplementations. Can we remove it?
    private PlanEnumeration planEnumeration;

    /**
     * Keep track of the {@link Platform}s of our {@link #operators}.
     */
    private Set<Platform> platformCache;

    /**
     * {@link OptimizationContext} that provides estimates for the {@link #operators}.
     */
    private final OptimizationContext optimizationContext;

    /**
     * The squashed cost estimate to execute this instance. This will be used to select the best plan!
     */
    private double squashedCostEstimateCache = Double.NaN, squashedCostEstimateWithoutOverheadCache = Double.NaN;

    /**
     * Create a new instance.
     */
    PlanImplementation(
            PlanEnumeration planEnumeration,
            Map<OutputSlot<?>, Junction> junctions,
            Collection<ExecutionOperator> operators,
            OptimizationContext optimizationContext) {
        this(planEnumeration, junctions, new Canonicalizer<>(operators), optimizationContext);
    }

    /**
     * Creates new instance.
     */
    PlanImplementation(PlanEnumeration planEnumeration,
                       Map<OutputSlot<?>, Junction> junctions,
                       OptimizationContext optimizationContext) {
        this(planEnumeration, junctions, new Canonicalizer<>(), optimizationContext);
    }

    /**
     * Copy constructor.
     */
    public PlanImplementation(PlanImplementation original) {
        this.planEnumeration = original.planEnumeration;
        this.junctions = new HashMap<>(original.junctions);
        this.operators = new Canonicalizer<>(original.getOperators());
        this.settledAlternatives.putAll(original.settledAlternatives);
        this.loopImplementations.putAll(original.loopImplementations);
        this.optimizationContext = original.optimizationContext;
    }

    /**
     * Base constructor.
     */
    private PlanImplementation(PlanEnumeration planEnumeration,
                               Map<OutputSlot<?>, Junction> junctions,
                               Canonicalizer<ExecutionOperator> operators,
                               OptimizationContext optimizationContext) {
        this.planEnumeration = planEnumeration;
        this.junctions = junctions;
        this.operators = operators;
        this.optimizationContext = optimizationContext;

        assert this.planEnumeration != null;
    }


    /**
     * @return the {@link PlanEnumeration} this instance belongs to
     */
    public PlanEnumeration getPlanEnumeration() {
        return this.planEnumeration;
    }

    public void setPlanEnumeration(PlanEnumeration planEnumeration) {
        this.planEnumeration = planEnumeration;
    }


    /**
     * Find the {@link InputSlot}s of already picked {@link ExecutionOperator}s that represent the given {@link InputSlot}.
     * <p>Note that we require that this instance either provides all or no {@link ExecutionOperator}s necessary to
     * implement the {@link InputSlot}.</p>
     *
     * @param someInput any {@link InputSlot} of the original {@link RheemPlan}
     * @return the representing {@link InputSlot}s or {@code null} if this instance has no {@link ExecutionOperator}
     * backing the given {@link InputSlot}
     */
    Collection<InputSlot<?>> findExecutionOperatorInputs(final InputSlot<?> someInput) {
        final Operator owner = someInput.getOwner();
        if (owner.isAlternative()) {
            final OperatorAlternative operatorAlternative = (OperatorAlternative) owner;
            final OperatorAlternative.Alternative alternative = this.settledAlternatives.get(operatorAlternative);
            if (alternative == null) return null;
            @SuppressWarnings("unchecked")
            final Collection<InputSlot<?>> innerInputs = (Collection<InputSlot<?>>) (Collection) alternative.followInput(someInput);
            boolean isWithNull = false;
            Collection<InputSlot<?>> result = null;
            for (InputSlot<?> innerInput : innerInputs) {
                final Collection<InputSlot<?>> resolvedInputs = this.findExecutionOperatorInputs(innerInput);
                if (isWithNull && resolvedInputs != null) {
                    throw new IllegalStateException(String.format("Disallowed that %s is required by two different alternatives.", someInput));
                }
                isWithNull |= resolvedInputs == null;
                if (result == null) {
                    result = resolvedInputs;
                } else {
                    assert resolvedInputs != null;
                    result.addAll(resolvedInputs);
                }
            }
            return result;

        } else if (owner.isLoopSubplan()) {
            final LoopSubplan loopSubplan = (LoopSubplan) owner;
            final LoopImplementation loopImplementation = this.getLoopImplementations().get(loopSubplan);
            if (loopImplementation == null) return null;

            // Enter the LoopSubplan.
            final Collection<InputSlot<?>> innerInputs = loopSubplan.followInputUnchecked(someInput);
            if (innerInputs.isEmpty()) return innerInputs;

            // Discern LoopHeadOperator InputSlots and loop body InputSlots.
            final List<LoopImplementation.IterationImplementation> iterationImpls = loopImplementation.getIterationImplementations();
            final Collection<InputSlot<?>> collector = new HashSet<>(innerInputs.size());
            for (InputSlot<?> innerInput : innerInputs) {
                if (innerInput.getOwner() == loopSubplan.getLoopHead()) {
                    final LoopImplementation.IterationImplementation initialIterationImpl = iterationImpls.get(0);
                    collector.addAll(
                            initialIterationImpl.getBodyImplementation().findExecutionOperatorInputs(innerInput)
                    );
                } else {
                    for (LoopImplementation.IterationImplementation iterationImpl : iterationImpls) {
                        collector.addAll(
                                iterationImpl.getBodyImplementation().findExecutionOperatorInputs(innerInput)
                        );
                    }
                }
            }
            return collector;

        } else {
            assert owner.isExecutionOperator();
            Collection<InputSlot<?>> result = new LinkedList<>();
            result.add(someInput);
            return result;
        }

    }

    /**
     * Find the {@link OutputSlot}s of already picked {@link ExecutionOperator}s that represent the given {@link OutputSlot}.
     *
     * @param someOutput any {@link OutputSlot} of the original {@link RheemPlan}
     * @return the representing {@link OutputSlot}s
     */
    Collection<OutputSlot<?>> findExecutionOperatorOutput(OutputSlot<?> someOutput) {
        return this.findExecutionOperatorOutputWithContext(someOutput).stream()
                .map(Tuple::getField0)
                .collect(Collectors.toList());
    }

    /**
     * Find the {@link OutputSlot}s of already picked {@link ExecutionOperator}s that represent the given {@link OutputSlot}.
     *
     * @param someOutput any {@link OutputSlot} of the original {@link RheemPlan}
     * @return the representing {@link OutputSlot}s together with their enclosing {@link PlanImplementation}
     */
    Collection<Tuple<OutputSlot<?>, PlanImplementation>> findExecutionOperatorOutputWithContext(
            OutputSlot<?> someOutput) {
        while (someOutput != null
                && someOutput.getOwner().isAlternative()) {
            final Operator owner = someOutput.getOwner();
            final OperatorAlternative operatorAlternative = (OperatorAlternative) owner;
            final OperatorAlternative.Alternative alternative = this.settledAlternatives.get(operatorAlternative);
            someOutput = alternative == null ? null : alternative.traceOutput(someOutput);
        }

        // If we did not find a terminal OutputSlot.
        if (someOutput == null) {
            return Collections.emptySet();
        }

        // Otherwise, discern LoopSubplans and ExecutionOperators.
        final Operator owner = someOutput.getOwner();
        if (owner.isLoopSubplan()) {
            final LoopSubplan loopSubplan = (LoopSubplan) owner;
            final LoopImplementation loopImplementation = this.getLoopImplementations().get(loopSubplan);
            if (loopImplementation == null) return Collections.emptyList();

            // Enter the LoopSubplan.
            final OutputSlot<?> innerOutput = loopSubplan.traceOutput(someOutput);
            if (innerOutput == null) return Collections.emptyList();
            assert innerOutput.getOwner().isLoopHead();

            // For all the iterations, return the potential OutputSlots.
            final List<LoopImplementation.IterationImplementation> iterationImpls =
                    loopImplementation.getIterationImplementations();
            final Set<Tuple<OutputSlot<?>, PlanImplementation>> collector = new HashSet<>(iterationImpls.size());
            for (LoopImplementation.IterationImplementation iterationImpl : iterationImpls) {
                final Collection<Tuple<OutputSlot<?>, PlanImplementation>> outputsWithContext =
                        iterationImpl.getBodyImplementation().findExecutionOperatorOutputWithContext(innerOutput);
                collector.addAll(outputsWithContext);
            }

            return collector;

        } else {
            assert owner.isExecutionOperator();
            return Collections.singleton(new Tuple<>(someOutput, this));
        }
    }


    /**
     * Creates a new instance that forms the concatenation of this instance with the {@code targetPlans} via the
     * {@code junction}.
     *
     * @param targetPlans              instances to connect to
     * @param junction                 connects this instance with the {@code targetPlans}
     * @param outputPlanImplementation nested instance of this instance that hosts the {@code junction}
     * @param concatenationEnumeration that will host the concatenated instance
     * @return the concatenated instance or {@code null} if the inputs are contradicting each other
     */
    PlanImplementation concatenate(List<PlanImplementation> targetPlans,
                                   Junction junction,
                                   PlanImplementation outputPlanImplementation,
                                   PlanEnumeration concatenationEnumeration) {

        final PlanImplementation concatenation = new PlanImplementation(
                concatenationEnumeration,
                new HashMap<>(this.junctions.size() + 1),
                new HashSet<>(this.settledAlternatives.size(), targetPlans.size() * 4), // ballpark figure
                this.optimizationContext
        );

        concatenation.operators.addAll(this.operators);
        concatenation.junctions.putAll(this.junctions);
        concatenation.settledAlternatives.putAll(this.settledAlternatives);

        // Find the appropriate PlanImplementation for the junction and copy the loop implementations.
        PlanImplementation junctionPlanImplementation;
        if (outputPlanImplementation == this) {
            // Special case: The junction resides inside the top-level PlanImplementation.
            concatenation.loopImplementations.putAll(this.loopImplementations);
            junctionPlanImplementation = concatenation;
        } else {
            // Exhaustively, yet focused, search for the PlanImplementation.
            junctionPlanImplementation = concatenation.copyLoopImplementations(
                    this,
                    outputPlanImplementation,
                    junction.getSourceOutput().getOwner().getLoopStack()
            );
        }
        junctionPlanImplementation.junctions.put(junction.getSourceOutput(), junction);

        for (PlanImplementation targetPlan : targetPlans) {
            // NB: Join semantics at this point weaved in.
            if (concatenation.isSettledAlternativesContradicting(targetPlan)) {
                return null;
            }
            concatenation.operators.addAll(targetPlan.operators);
            concatenation.loopImplementations.putAll(targetPlan.loopImplementations);
            concatenation.junctions.putAll(targetPlan.junctions);
            concatenation.settledAlternatives.putAll(targetPlan.settledAlternatives);
        }

        return concatenation;
    }

    /**
     * Find the a given nested {@link PlanImplementation} in a further {@link PlanImplementation} and copy it to
     * this instance.
     *
     * @param originalPlanImplementation the (top-level) {@link PlanImplementation} to copy from
     * @param targetPlanImplementation   the (nested) {@link PlanImplementation} that should be copied
     * @param loopStack                  of an {@link ExecutionOperator} inside of the {@code targetPlanImplementation}
     * @return the copied {@link PlanImplementation} inside of this instance
     */
    private PlanImplementation copyLoopImplementations(PlanImplementation originalPlanImplementation,
                                                       PlanImplementation targetPlanImplementation,
                                                       LinkedList<LoopSubplan> loopStack) {
        // Descend into the loopStack.
        assert !loopStack.isEmpty();
        final LoopSubplan visitedLoop = loopStack.pop();

        // Copy the LoopImplementations of the originalPlanImplementation.
        this.loopImplementations.putAll(originalPlanImplementation.getLoopImplementations());
        // This one will be altered, so make an instance copy.
        final LoopImplementation loopImplCopy =
                this.loopImplementations.compute(visitedLoop, (key, value) -> new LoopImplementation(value));

        // Find the original counterpart to loopImplCopy.
        final LoopImplementation originalLoopImpl = originalPlanImplementation.loopImplementations.get(visitedLoop);


        // Go over the iterations of the LoopImplementations in parallel to process their PlanImplementations.
        PlanImplementation targetPlanImplementationCopy = null;
        Iterator<LoopImplementation.IterationImplementation>
                originalIterator = originalLoopImpl.getIterationImplementations().iterator(),
                copyIterator = loopImplCopy.getIterationImplementations().iterator();
        while (originalIterator.hasNext()) {
            final LoopImplementation.IterationImplementation nextCopy = copyIterator.next();
            final LoopImplementation.IterationImplementation nextOriginal = originalIterator.next();
            // If we need to descend further, invoke a recursive call.
            if (!loopStack.isEmpty()) {
                targetPlanImplementationCopy = nextCopy.getBodyImplementation().copyLoopImplementations(
                        nextOriginal.getBodyImplementation(),
                        targetPlanImplementation,
                        loopStack);

                // Once, we have found the iteration that contains the targetPlanImplementation, we can stop.
                if (targetPlanImplementationCopy != null) break;
            } else {
                // If we cannot descend futher, we basically need to find the correct iteration only.
                if (nextOriginal.getBodyImplementation() == targetPlanImplementation) {
                    targetPlanImplementationCopy = nextCopy.getBodyImplementation();
                    break;
                }
            }
        }

        // Restore the loopStack.
        loopStack.push(visitedLoop);

        // Return the match.
        return targetPlanImplementationCopy;


    }

    private boolean isSettledAlternativesContradicting(PlanImplementation that) {
        for (Map.Entry<OperatorAlternative, OperatorAlternative.Alternative> entry : this.settledAlternatives.entrySet()) {
            final OperatorAlternative opAlt = entry.getKey();
            final OperatorAlternative.Alternative alternative = entry.getValue();
            final OperatorAlternative.Alternative thatAlternative = that.settledAlternatives.get(opAlt);
            if (thatAlternative != null && alternative != thatAlternative) {
                return true;
            }
        }
        for (Map.Entry<LoopSubplan, LoopImplementation> entry : this.loopImplementations.entrySet()) {
            final LoopSubplan loop = entry.getKey();
            final LoopImplementation thisLoopImplementation = entry.getValue();
            final LoopImplementation thatLoopImplementation = that.loopImplementations.get(loop);
            if (thatLoopImplementation == null) continue;
            if (thisLoopImplementation
                    .getSingleIterationImplementation()
                    .getBodyImplementation()
                    .isSettledAlternativesContradicting(
                            thatLoopImplementation
                                    .getIterationImplementations().get(0)
                                    .getBodyImplementation()
                    )) {
                return true;
            }
        }
        return false;
    }

    /**
     * Escapes the {@link OperatorAlternative} that contains this instance.
     *
     * @param alternative        contains this instance
     * @param newPlanEnumeration will host the new instance
     * @return
     */
    public PlanImplementation escape(OperatorAlternative.Alternative alternative, PlanEnumeration newPlanEnumeration) {
        final PlanImplementation escapedPlanImplementation = new PlanImplementation(
                newPlanEnumeration, this.junctions, this.operators, this.optimizationContext
        );
        escapedPlanImplementation.settledAlternatives.putAll(this.settledAlternatives);
        assert !escapedPlanImplementation.settledAlternatives.containsKey(alternative.getOperatorAlternative());
        escapedPlanImplementation.settledAlternatives.put(alternative.getOperatorAlternative(), alternative);
        escapedPlanImplementation.loopImplementations.putAll(this.getLoopImplementations());
        return escapedPlanImplementation;
    }

    public Canonicalizer<ExecutionOperator> getOperators() {
        return this.operators;
    }

    public Map<LoopSubplan, LoopImplementation> getLoopImplementations() {
        return this.loopImplementations;
    }

    /**
     * Adds a new {@link LoopImplementation} for a given {@link LoopSubplan}.
     *
     * @param loop               the {@link LoopSubplan}
     * @param loopImplementation the {@link LoopImplementation}
     */
    public void addLoopImplementation(LoopSubplan loop, LoopImplementation loopImplementation) {
        this.loopImplementations.put(loop, loopImplementation);
    }

    /**
     * @return those contained {@link ExecutionOperator}s that have a {@link Slot} that is yet to be connected
     * to a further {@link ExecutionOperator} in the further plan enumeration process
     */
    public Collection<ExecutionOperator> getInterfaceOperators() {
        Validate.notNull(this.getPlanEnumeration());
        final Set<OutputSlot> outputSlots = this.getPlanEnumeration().servingOutputSlots.stream()
                .map(Tuple::getField0)
                .distinct()
                .collect(Collectors.toSet());
        final Set<InputSlot<?>> inputSlots = this.getPlanEnumeration().requestedInputSlots;

        return this.operators.stream()
                .filter(operator ->
                        this.allOutermostInputSlots(operator).anyMatch(inputSlots::contains) ||
                                this.allOutermostOutputSlots(operator).anyMatch(outputSlots::contains))
                .collect(Collectors.toList());
    }

    private Stream<OutputSlot> allOutermostOutputSlots(Operator operator) {
        return Arrays.stream(operator.getAllOutputs())
                .flatMap(output -> operator.getOutermostOutputSlots(output).stream());
    }

    private Stream<InputSlot> allOutermostInputSlots(Operator operator) {
        return Arrays.stream(operator.getAllInputs())
                .map(operator::getOutermostInputSlot);
    }

    /**
     * Find the {@link ExecutionOperator} that do not depend on any other {@link ExecutionOperator} as input.
     *
     * @return the start {@link ElementaryOperator}s
     */
    public List<ExecutionOperator> getStartOperators() {
        return this.operators.stream()
                .filter(this::isStartOperator)
                .collect(Collectors.toList());
    }

    /**
     * Detects start {@link ExecutionOperator}s.
     * <p>A start {@link ExecutionOperator} has an {@link InputSlot} that is requested by the {@link #planEnumeration}.</p>
     */
    private boolean isStartOperator(ExecutionOperator executionOperator) {
        ForLoop:
        for (InputSlot<?> inputSlot : executionOperator.getOriginal().getAllInputs()) {
            while (inputSlot != null) {
                if (this.planEnumeration.requestedInputSlots.contains(inputSlot)) {
                    continue ForLoop;
                }
                inputSlot = inputSlot.getOwner().getOuterInputSlot(inputSlot);
            }
            return false;
        }
        return true;
    }

    /**
     * Find for a given {@link OperatorAlternative}, which {@link OperatorAlternative.Alternative} has been picked
     * by this instance
     *
     * @param operatorAlternative the {@link OperatorAlternative} in question
     * @return the {@link OperatorAlternative.Alternative} or {@code null} if none has been chosen in this instance
     */
    public OperatorAlternative.Alternative getChosenAlternative(OperatorAlternative operatorAlternative) {
        return this.settledAlternatives.get(operatorAlternative);
    }

    /**
     * Retrieves the {@link TimeEstimate} for this instance, including platform overhead.
     *
     * @return the {@link TimeEstimate}
     */
    public TimeEstimate getTimeEstimate() {
        return this.getTimeEstimate(true);
    }

    /**
     * Retrieves the {@link TimeEstimate} for this instance, including platform overhead.
     *
     * @param isIncludeOverhead whether to include any incurring global overhead
     * @return the {@link TimeEstimate}
     */
    public TimeEstimate getTimeEstimate(boolean isIncludeOverhead) {
        final TimeEstimate operatorTimeEstimate = this.operators.stream()
                .map(op -> this.optimizationContext.getOperatorContext(op).getTimeEstimate())
                .reduce(TimeEstimate.ZERO, TimeEstimate::plus);
        final TimeEstimate junctionTimeEstimate = this.optimizationContext.getDefaultOptimizationContexts().stream()
                .flatMap(optCtx -> this.junctions.values().stream().map(jct -> jct.getTimeEstimate(optCtx)))
                .reduce(TimeEstimate.ZERO, TimeEstimate::plus);
        final TimeEstimate loopTimeEstimate = this.loopImplementations.values().stream()
                .map(LoopImplementation::getTimeEstimate)
                .reduce(TimeEstimate.ZERO, TimeEstimate::plus);
        TimeEstimate timeEstimate = operatorTimeEstimate.plus(junctionTimeEstimate).plus(loopTimeEstimate);

        if (isIncludeOverhead) {
            final long platformInitializationTime = this.getUtilizedPlatforms().stream()
                    .map(platform -> this.optimizationContext.getConfiguration().getPlatformStartUpTimeProvider().provideFor(platform))
                    .reduce(0L, (a, b) -> a + b);
            timeEstimate = timeEstimate.plus(platformInitializationTime);
        }

        return timeEstimate;
    }

    /**
     * Retrieves the cost estimate for this instance including any overhead.
     *
     * @return the cost estimate
     */
    public ProbabilisticDoubleInterval getCostEstimate() {
        return this.getCostEstimate(true);
    }

    /**
     * Retrieves the cost estimate for this instance.
     *
     * @param isIncludeOverhead whether to include global overhead in the {@link TimeEstimate} (to avoid repeating
     *                          overhead in nested instances)
     * @return the cost estimate
     */
    ProbabilisticDoubleInterval getCostEstimate(boolean isIncludeOverhead) {
        ProbabilisticDoubleInterval costEstimateWithoutOverheadCache, costEstimateCache;
        final ProbabilisticDoubleInterval operatorCosts = this.operators.stream()
                .map(op -> this.optimizationContext.getOperatorContext(op).getCostEstimate())
                .reduce(ProbabilisticDoubleInterval.zero, ProbabilisticDoubleInterval::plus);
        final ProbabilisticDoubleInterval junctionCosts = this.optimizationContext.getDefaultOptimizationContexts().stream()
                .flatMap(optCtx -> this.junctions.values().stream().map(jct -> jct.getCostEstimate(optCtx)))
                .reduce(ProbabilisticDoubleInterval.zero, ProbabilisticDoubleInterval::plus);
        final ProbabilisticDoubleInterval loopCosts = this.loopImplementations.values().stream()
                .map(LoopImplementation::getCostEstimate)
                .reduce(ProbabilisticDoubleInterval.zero, ProbabilisticDoubleInterval::plus);
        costEstimateWithoutOverheadCache = operatorCosts.plus(junctionCosts).plus(loopCosts);
        ProbabilisticDoubleInterval overheadCosts = this.getUtilizedPlatforms().stream()
                .map(platform -> {
                    Configuration configuraiton = this.optimizationContext.getConfiguration();
                    long startUpTime = configuraiton.getPlatformStartUpTimeProvider().provideFor(platform);
                    TimeToCostConverter timeToCostConverter = configuraiton.getTimeToCostConverterProvider().provideFor(platform);
                    return timeToCostConverter.convert(new TimeEstimate(startUpTime, startUpTime, 1d));
                })
                .reduce(ProbabilisticDoubleInterval.zero, ProbabilisticDoubleInterval::plus);
        costEstimateCache = costEstimateWithoutOverheadCache.plus(overheadCosts);
        return isIncludeOverhead ? costEstimateCache : costEstimateWithoutOverheadCache;
    }

    /**
     * Retrieves the cost estimate for this instance including any overhead.
     *
     * @return the cost estimate
     */
    public double getSquashedCostEstimate() {
        return this.getSquashedCostEstimate(true);
    }

    /**
     * Retrieves the cost estimate for this instance.
     *
     * @param isIncludeOverhead whether to include global overhead in the {@link TimeEstimate} (to avoid repeating
     *                          overhead in nested instances)
     * @return the squashed cost estimate
     */
    double getSquashedCostEstimate(boolean isIncludeOverhead) {
        assert Double.isNaN(this.squashedCostEstimateCache) == Double.isNaN(this.squashedCostEstimateWithoutOverheadCache);
        if (Double.isNaN(this.squashedCostEstimateCache)) {
            final double operatorCosts = this.operators.stream()
                    .mapToDouble(op -> this.optimizationContext.getOperatorContext(op).getSquashedCostEstimate())
                    .sum();
            final double junctionCosts = this.optimizationContext.getDefaultOptimizationContexts().stream()
                    .flatMapToDouble(optCtx -> this.junctions.values().stream().mapToDouble(jct -> jct.getSquashedCostEstimate(optCtx)))
                    .sum();
            final double loopCosts = this.loopImplementations.values().stream()
                    .mapToDouble(LoopImplementation::getSquashedCostEstimate)
                    .sum();
            this.squashedCostEstimateWithoutOverheadCache = operatorCosts + junctionCosts + loopCosts;
            double overheadCosts = this.getUtilizedPlatforms().stream()
                    .mapToDouble(platform -> {
                        Configuration configuration = this.optimizationContext.getConfiguration();

                        long startUpTime = configuration.getPlatformStartUpTimeProvider().provideFor(platform);

                        TimeToCostConverter timeToCostConverter = configuration.getTimeToCostConverterProvider().provideFor(platform);
                        ProbabilisticDoubleInterval costs = timeToCostConverter.convert(new TimeEstimate(startUpTime, startUpTime, 1d));

                        final ToDoubleFunction<ProbabilisticDoubleInterval> squasher = configuration.getCostSquasherProvider().provide();
                        return squasher.applyAsDouble(costs);
                    })
                    .sum();
            this.squashedCostEstimateCache = this.squashedCostEstimateWithoutOverheadCache + overheadCosts;
        }
        return isIncludeOverhead ? this.squashedCostEstimateCache : this.squashedCostEstimateWithoutOverheadCache;
    }

    public Junction getJunction(OutputSlot<?> output) {
        return this.junctions.get(output);
    }

    public void putJunction(OutputSlot<?> output, Junction junction) {
        final Junction oldValue = junction == null ?
                this.junctions.remove(output) :
                this.junctions.put(output, junction);
        if (oldValue != null) {
            logger.warn("Replaced {} with {}.", oldValue, junction);
        }
    }

    public OptimizationContext getOptimizationContext() {
        return this.optimizationContext;
    }

    /**
     * Merges the {@link OptimizationContext}s of the {@link Junction}s in this instance into its main
     * {@link OptimizationContext}/
     */
    public void mergeJunctionOptimizationContexts() {
        // Merge the top-level Junctions.
        for (Junction junction : this.junctions.values()) {
            junction.getOptimizationContexts().forEach(OptimizationContext::mergeToBase);
        }

        // Descend into loops.
        this.loopImplementations.values().stream()
                .flatMap(loopImplementation -> loopImplementation.getIterationImplementations().stream())
                .map(LoopImplementation.IterationImplementation::getBodyImplementation)
                .forEach(PlanImplementation::mergeJunctionOptimizationContexts);
    }

    public void logTimeEstimates() {
        if (!this.logger.isDebugEnabled()) return;

        this.logger.debug(">>> Regular operators");
        for (ExecutionOperator operator : this.operators) {
            this.logger.debug("Estimated execution time of {}: {}",
                    operator, this.optimizationContext.getOperatorContext(operator).getTimeEstimate()
            );
        }
        this.logger.debug(">>> Glue operators");
        for (Junction junction : junctions.values()) {
            for (ExecutionTask task : junction.getConversionTasks()) {
                final ExecutionOperator operator = task.getOperator();
                this.logger.debug("Estimated execution time of {}: {}",
                        operator, this.optimizationContext.getOperatorContext(operator).getTimeEstimate()
                );
            }
        }
        this.logger.debug(">>> Loops");
        for (LoopImplementation loopImplementation : this.loopImplementations.values()) {
            for (LoopImplementation.IterationImplementation iterationImplementation : loopImplementation.getIterationImplementations()) {
                iterationImplementation.getBodyImplementation().logTimeEstimates();
            }
        }
    }

    /**
     * Retrieve the {@link Platform}s that are utilized by this instance.
     *
     * @return the {@link Platform}s
     */
    public Set<Platform> getUtilizedPlatforms() {
        if (this.platformCache == null) {
            this.platformCache = this.streamOperators()
                    .map(ExecutionOperator::getPlatform)
                    .collect(Collectors.toSet());
        }
        return this.platformCache;
    }

    /**
     * Stream all the {@link ExecutionOperator}s in this instance.
     *
     * @return a {@link Stream} containing every {@link ExecutionOperator} at least once
     */
    Stream<ExecutionOperator> streamOperators() {
        Stream<ExecutionOperator> operatorStream = Stream.concat(
                this.operators.stream(),
                this.junctions.values().stream().flatMap(j -> j.getConversionTasks().stream()).map(ExecutionTask::getOperator)
        );
        if (!this.loopImplementations.isEmpty()) {
            operatorStream = Stream.concat(
                    operatorStream,
                    this.loopImplementations.values().stream().flatMap(LoopImplementation::streamOperators)
            );
        }
        return operatorStream;
    }

    @Override
    public String toString() {
        return String.format("PlanImplementation[%s, %s, costs=%s]",
                this.getUtilizedPlatforms(), this.getTimeEstimate(), this.getCostEstimate()
        );
    }
}
