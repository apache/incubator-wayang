package org.qcri.rheem.core.api;

import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.mapping.PlanTransformation;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimatorManager;
import org.qcri.rheem.core.optimizer.costs.TimeEstimate;
import org.qcri.rheem.core.optimizer.enumeration.PartialPlan;
import org.qcri.rheem.core.optimizer.enumeration.PlanEnumeration;
import org.qcri.rheem.core.optimizer.enumeration.PlanEnumerator;
import org.qcri.rheem.core.optimizer.enumeration.StageAssignmentTraversal;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionPlan;
import org.qcri.rheem.core.plan.executionplan.ExecutionStage;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.platform.CardinalityBreakpoint;
import org.qcri.rheem.core.platform.CrossPlatformExecutor;
import org.qcri.rheem.core.platform.ExecutionProfile;
import org.qcri.rheem.core.platform.FixBreakpoint;
import org.qcri.rheem.core.profiling.CardinalityRepository;
import org.qcri.rheem.core.profiling.InstrumentationStrategy;
import org.qcri.rheem.core.util.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Describes a job that is to be executed using Rheem.
 */
public class Job {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * Guardian to avoid re-execution.
     */
    private final AtomicBoolean hasBeenExecuted = new AtomicBoolean(false);

    /**
     * References the {@link RheemContext} that spawned this instance.
     */
    private final RheemContext rheemContext;

    /**
     * {@link Job}-level {@link Configuration} based on the {@link RheemContext}-level configuration.
     */
    private final Configuration configuration;

    /**
     * The {@link RheemPlan} to be executed by this instance.
     */
    private final RheemPlan rheemPlan;

    /**
     * {@link OptimizationContext} for the {@link #rheemPlan}.
     */
    private OptimizationContext optimizationContext;

    /**
     * Executes the optimized {@link ExecutionPlan}.
     */
    private CrossPlatformExecutor crossPlatformExecutor;

    /**
     * Manages the {@link CardinalityEstimate}s for the {@link #rheemPlan}.
     */
    private CardinalityEstimatorManager cardinalityEstimatorManager;

    /**
     * {@link StopWatch} to measure some key figures.
     */
    private final StopWatch stopWatch = new StopWatch();

    private final double minConfidence = 5., maxSpread = .7;

    private final StageAssignmentTraversal.StageSplittingCriterion stageSplittingCriterion =
            (producerTask, channel, consumerTask) -> {
                final CardinalityEstimate ce = channel.getCardinalityEstimate();
                return ce.getCorrectnessProbability() >= this.minConfidence
                        && CardinalityBreakpoint.calculateSpread(ce) <= this.maxSpread;
            };


    /**
     * Creates a new instance.
     */
    Job(RheemContext rheemContext, RheemPlan rheemPlan) {
        this.rheemContext = rheemContext;
        this.configuration = this.rheemContext.getConfiguration().fork();
        this.rheemPlan = rheemPlan;
    }

    /**
     * Execute this job.
     */
    public void execute() {
        // Make sure that each job is only executed once.
        if (this.hasBeenExecuted.getAndSet(true)) {
            throw new RheemException("Job has already been executed.");
        }

        try {
            // Prepare the #rheemPlan for the optimization.
            this.prepareRheemPlan();

            // Estimate cardinalities and execution times for the #rheemPlan.
            this.estimateKeyFigures();

            // Get an execution plan.
            ExecutionPlan executionPlan = this.createInitialExecutionPlan();

            // Take care of the execution.
            int executionId = 0;
            CrossPlatformExecutor.State state = null;
            while (state == null || !state.isComplete()) {
                state = this.execute(executionPlan, executionId);
                this.postProcess(executionPlan, state, executionId);
                executionId++;
            }
        } finally {
            this.stopWatch.stopAll();
            this.stopWatch.start("Release Resources");
            this.releaseResources();
            this.stopWatch.stop("Release Resources");
            this.logger.info("StopWatch results:\n{}", this.stopWatch.toPrettyString());
        }
    }

    /**
     * Prepares the {@link #rheemPlan}: prunes unused {@link Operator}s, isolates loops, and applies all available
     * {@link PlanTransformation}s.
     */
    private void prepareRheemPlan() {

        // Prepare the RheemPlan for the optimization.
        this.stopWatch.start("Prepare", "Prune&Isolate");
        this.rheemPlan.prepare();
        this.stopWatch.stop("Prepare", "Prune&Isolate");

        // Apply the mappings to the plan to form a hyperplan.
        this.stopWatch.start("Prepare", "Transformations");
        final Collection<PlanTransformation> transformations = this.gatherTransformations();
        this.rheemPlan.applyTransformations(transformations);
        this.stopWatch.stop("Prepare", "Transformations");

        this.stopWatch.start("Prepare", "Sanity");
        assert this.rheemPlan.isSane();
        this.stopWatch.stop("Prepare", "Sanity");

        this.stopWatch.stopAll("Prepare");
    }

    /**
     * Gather all available {@link PlanTransformation}s from the {@link #configuration}.
     */
    private Collection<PlanTransformation> gatherTransformations() {
        return this.configuration.getPlatformProvider().provideAll().stream()
                .flatMap(platform -> platform.getMappings().stream())
                .flatMap(mapping -> mapping.getTransformations().stream())
                .collect(Collectors.toList());
    }


    /**
     * Go over the given {@link RheemPlan} and estimate the cardinalities of data being passed between its
     * {@link Operator}s and the execution profile and time of {@link ExecutionOperator}s.
     */
    private void estimateKeyFigures() {
        this.stopWatch.start("Cardinality&Load Estimation");
        if (this.cardinalityEstimatorManager == null) {
            this.stopWatch.start("Cardinality&Load Estimation", "Create OptimizationContext");
            this.optimizationContext = new OptimizationContext(this.rheemPlan);
            this.stopWatch.stop("Cardinality&Load Estimation", "Create OptimizationContext");

            this.stopWatch.start("Cardinality&Load Estimation", "Create CardinalityEstimationManager");
            this.cardinalityEstimatorManager = new CardinalityEstimatorManager(
                    this.rheemPlan, this.optimizationContext, this.configuration);
            this.stopWatch.stop("Cardinality&Load Estimation", "Create CardinalityEstimationManager");
        }

        this.stopWatch.start("Cardinality&Load Estimation", "Push Estimation");
        this.cardinalityEstimatorManager.pushCardinalities();
        this.stopWatch.stop("Cardinality&Load Estimation", "Push Estimation");

        this.stopWatch.stopAll("Cardinality&Load Estimation");
    }


    /**
     * Determine a good/the best execution plan from a given {@link RheemPlan}.
     */
    private ExecutionPlan createInitialExecutionPlan() {
        this.stopWatch.start("Create Initial Execution Plan");

        // Defines the plan that we want to use in the end.
        final Comparator<TimeEstimate> timeEstimateComparator = this.configuration.getTimeEstimateComparatorProvider().provide();

        // Enumerate all possible plan.
        final PlanEnumerator planEnumerator = this.createPlanEnumerator(null);

        this.stopWatch.start("Create Initial Execution Plan", "Enumerate");
        final PlanEnumeration comprehensiveEnumeration = planEnumerator.enumerate(true);
        this.stopWatch.stop("Create Initial Execution Plan", "Enumerate");

        final Collection<PartialPlan> executionPlans = comprehensiveEnumeration.getPartialPlans();
        this.logger.info("Enumerated {} plans.", executionPlans.size());
        for (PartialPlan partialPlan : executionPlans) {
            this.logger.debug("Plan with operators: {}", partialPlan.getOperators());
        }

        // Pick an execution plan.
        // Make sure that an execution plan can be created.
        this.stopWatch.start("Create Initial Execution Plan", "Pick Best Plan");
        final PartialPlan partialPlan = this.pickBestExecutionPlan(timeEstimateComparator, executionPlans, null, null, null);
        this.stopWatch.stop("Create Initial Execution Plan", "Pick Best Plan");

        this.stopWatch.start("Create Initial Execution Plan", "Split Stages");
        final ExecutionPlan executionPlan = partialPlan.getExecutionPlan().toExecutionPlan(this.stageSplittingCriterion);
        this.stopWatch.stop("Create Initial Execution Plan", "Split Stages");

        assert executionPlan.isSane();


        this.stopWatch.stopAll("Create Initial Execution Plan");
        return executionPlan;
    }


    private PartialPlan pickBestExecutionPlan(Comparator<TimeEstimate> timeEstimateComparator,
                                              Collection<PartialPlan> executionPlans,
                                              ExecutionPlan existingPlan,
                                              Set<Channel> openChannels,
                                              Set<ExecutionStage> executedStages) {

        return executionPlans.stream()
                .filter(plan -> (existingPlan == null ?
                        plan.createExecutionPlan() :
                        plan.createExecutionPlan(existingPlan, openChannels, executedStages)) != null)
                .reduce((p1, p2) -> {
                    final TimeEstimate t1 = p1.getExecutionPlan().estimateExecutionTime(this.configuration);
                    final TimeEstimate t2 = p2.getExecutionPlan().estimateExecutionTime(this.configuration);
                    return timeEstimateComparator.compare(t1, t2) > 0 ? p1 : p2;
                })
                .map(plan -> {
                    this.logger.info("Picked plan's cost estimate is {}.", plan.getExecutionPlan().estimateExecutionTime(this.configuration));
                    return plan;
                })
                .orElseThrow(() -> new IllegalStateException("Could not find an execution plan."));
    }

    /**
     * Go over the given {@link RheemPlan} and update the cardinalities of data being passed between its
     * {@link Operator}s using the given {@link ExecutionProfile}.
     */
    private void reestimateCardinalities(CrossPlatformExecutor.State executionState) {
        this.cardinalityEstimatorManager.pushCardinalityUpdates(executionState);
    }

    /**
     * Creates a new {@link PlanEnumerator} for the {@link #rheemPlan} and {@link #configuration}.
     */
    private PlanEnumerator createPlanEnumerator(ExecutionPlan existingPlan) {
        final PlanEnumerator planEnumerator = existingPlan == null ?
                new PlanEnumerator(this.rheemPlan, this.configuration) :
                new PlanEnumerator(this.rheemPlan, this.configuration, existingPlan);
        this.configuration.getPruningStrategiesProvider().forEach(planEnumerator::addPruningStrategy);
        return planEnumerator;
    }

    /**
     * Dummy implementation: Have the platforms execute the given execution plan.
     */
    private CrossPlatformExecutor.State execute(ExecutionPlan executionPlan, int executionId) {
        final StopWatch.Round round = this.stopWatch.start(String.format("Execution %d", executionId));

        // Set up appropriate Breakpoints.
        final StopWatch.Round breakpointRound = round.startSubround("Configure Breakpoint");
        FixBreakpoint breakpoint = new FixBreakpoint();
        if (this.crossPlatformExecutor == null) {
            final InstrumentationStrategy instrumentation = this.configuration.getInstrumentationStrategyProvider().provide();
            this.crossPlatformExecutor = new CrossPlatformExecutor(instrumentation);
            executionPlan.getStartingStages().forEach(breakpoint::breakAfter);

        } else {
            final CrossPlatformExecutor.State state = this.crossPlatformExecutor.captureState();
            state.getCompletedStages().stream()
                    .flatMap(stage -> stage.getSuccessors().stream())
                    .filter(stage -> !state.getCompletedStages().contains(stage))
                    .forEach(breakpoint::breakAfter);
        }
        this.crossPlatformExecutor.extendBreakpoint(breakpoint);
        this.crossPlatformExecutor.extendBreakpoint(new CardinalityBreakpoint(this.maxSpread, this.minConfidence));
        breakpointRound.stop();

        // Trigger the execution.
        final StopWatch.Round executeRound = round.startSubround("Execute");
        final CrossPlatformExecutor.State state = this.crossPlatformExecutor.executeUntilBreakpoint(executionPlan);
        executeRound.stop();
        round.stop(true, true);

        // Return the state.
        return state;
    }

    /**
     * Injects the cardinalities obtained from {@link Channel} instrumentation, potentially updates the {@link ExecutionPlan}
     * through re-optimization, and collects measured data.
     */
    private void postProcess(ExecutionPlan executionPlan, CrossPlatformExecutor.State state, int executionId) {
        final StopWatch.Round round = this.stopWatch.start(String.format("Post-processing %d", executionId));

        round.startSubround("Reestimate Cardinalities&Time");
        this.reestimateCardinalities(state);
        round.stopSubround("Reestimate Cardinalities&Time");

        if (!state.isComplete()) {
            round.startSubround("Update Execution Plan");
            this.updateExecutionPlan(executionPlan, state);
            round.stopSubround("Update Execution Plan");

        }

        // Collect any instrumentation results for the future.
        round.startSubround("Store Cardinalities");
        final CardinalityRepository cardinalityRepository = this.rheemContext.getCardinalityRepository();
        cardinalityRepository.storeAll(state.getProfile(), this.optimizationContext);
        round.stopSubround("Update Execution Plan");

        round.stop(true, true);
    }

    /**
     * Enumerate possible execution plans from the given {@link RheemPlan} and determine the (seemingly) best one.
     */
    private void updateExecutionPlan(ExecutionPlan executionPlan, CrossPlatformExecutor.State state) {
        // Defines the plan that we want to use in the end.
        final Comparator<TimeEstimate> timeEstimateComparator = this.configuration.getTimeEstimateComparatorProvider().provide();

        // Find and copy the open Channels.
        final Set<ExecutionStage> completedStages = state.getCompletedStages();
        final Set<ExecutionTask> completedTasks = completedStages.stream()
                .flatMap(stage -> stage.getAllTasks().stream())
                .collect(Collectors.toSet());

        // Find Channels that have yet to be consumed by unexecuted ExecutionTasks.
        // This must be done before scrapping the unexecuted ExecutionTasks!
        final Set<Channel> openChannels = completedTasks.stream()
                .flatMap(task -> Arrays.stream(task.getOutputChannels()))
                .filter(channel -> channel.getConsumers().stream().anyMatch(consumer -> !completedTasks.contains(consumer)))
                .collect(Collectors.toSet());

        // Scrap unexecuted bits of the plan.
        executionPlan.retain(completedStages);

        // Enumerate all possible plan.
        final PlanEnumerator planEnumerator = this.createPlanEnumerator(executionPlan);
        final PlanEnumeration comprehensiveEnumeration = planEnumerator.enumerate(true);
        final Collection<PartialPlan> executionPlans = comprehensiveEnumeration.getPartialPlans();
        this.logger.info("Enumerated {} plans.", executionPlans.size());
        for (PartialPlan partialPlan : executionPlans) {
            this.logger.debug("Plan with operators: {}", partialPlan.getOperators());
        }

        // Pick an execution plan.
        // Make sure that an execution plan can be created.
        final PartialPlan partialPlan = this.pickBestExecutionPlan(timeEstimateComparator, executionPlans, executionPlan,
                openChannels, completedStages);

        final ExecutionPlan executionPlanExpansion = partialPlan.getExecutionPlan().toExecutionPlan(this.stageSplittingCriterion);
        executionPlan.expand(executionPlanExpansion);

        assert executionPlan.isSane();
    }

    /**
     * Asks this instance to release its critical resources to avoid resource leaks and to enhance durability and
     * consistency of accessed resources.
     */
    private void releaseResources() {
        this.rheemContext.getCardinalityRepository().sleep();
        if (this.crossPlatformExecutor != null) this.crossPlatformExecutor.shutdown();
    }

    /**
     * Modify the {@link Configuration} to control the {@link Job} execution.
     */
    public Configuration getConfiguration() {
        return this.configuration;
    }
}
