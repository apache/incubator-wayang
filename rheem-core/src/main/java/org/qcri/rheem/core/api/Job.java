package org.qcri.rheem.core.api;

import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.mapping.PlanTransformation;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimatorManager;
import org.qcri.rheem.core.optimizer.costs.TimeEstimate;
import org.qcri.rheem.core.optimizer.enumeration.*;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionPlan;
import org.qcri.rheem.core.plan.executionplan.ExecutionStage;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.platform.*;
import org.qcri.rheem.core.profiling.CardinalityRepository;
import org.qcri.rheem.core.profiling.InstrumentationStrategy;
import org.qcri.rheem.core.util.OneTimeExecutable;
import org.qcri.rheem.core.util.ReflectionUtils;
import org.qcri.rheem.core.util.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Describes a job that is to be executed using Rheem.
 */
public class Job extends OneTimeExecutable {

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

    /**
     * JAR files that are needed to execute the UDFs.
     */
    private final Set<String> udfJarPaths = new HashSet<>();


    /**
     * <i>Currently not used.</i>
     */
    private final StageAssignmentTraversal.StageSplittingCriterion stageSplittingCriterion =
            (producerTask, channel, consumerTask) -> false;

    /**
     * Creates a new instance.
     *
     * @param udfJars paths to JAR files needed to run the UDFs (see {@link ReflectionUtils#getDeclaringJar(Class)})
     */
    Job(RheemContext rheemContext, RheemPlan rheemPlan, String... udfJars) {
        this.rheemContext = rheemContext;
        this.configuration = this.rheemContext.getConfiguration().fork();
        this.rheemPlan = rheemPlan;
        for (String udfJar : udfJars) {
            this.addUdfJar(udfJar);
        }
    }

    /**
     * Adds a {@code path} to a JAR that is required in one or more UDFs.
     *
     * @see ReflectionUtils#getDeclaringJar(Class)
     */
    public void addUdfJar(String path) {
        this.udfJarPaths.add(path);
    }

    /**
     * Run this instance. Must only be called once.
     * @throws RheemException in case the execution fails for any reason
     */
    @Override
    public void execute() throws RheemException {
        try {
            super.execute();
        } catch (RheemException e) {
            throw e;
        } catch (Throwable t) {
            throw new RheemException("Job execution failed.", t);
        }
    }

    @Override
    protected void doExecute() {
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
            while (!this.execute(executionPlan, executionId)) {
                this.postProcess(executionPlan, executionId);
                executionId++;
            }
        } catch (RheemException e) {
            throw e;
        } catch (Throwable t) {
            throw new RheemException("Job execution failed.", t);
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
            this.optimizationContext = new OptimizationContext(this.rheemPlan, this.configuration);
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
        final PlanEnumerator planEnumerator = this.createPlanEnumerator();

        this.stopWatch.start("Create Initial Execution Plan", "Enumerate");
        final PlanEnumeration comprehensiveEnumeration = planEnumerator.enumerate(true);
        this.stopWatch.stop("Create Initial Execution Plan", "Enumerate");

        final Collection<PlanImplementation> executionPlans = comprehensiveEnumeration.getPlanImplementations();
        this.logger.info("Enumerated {} plans.", executionPlans.size());
        for (PlanImplementation planImplementation : executionPlans) {
            this.logger.debug("Plan with operators: {}", planImplementation.getOperators());
        }

        // Pick an execution plan.
        // Make sure that an execution plan can be created.
        this.stopWatch.start("Create Initial Execution Plan", "Pick Best Plan");
        final PlanImplementation planImplementation = this.pickBestExecutionPlan(timeEstimateComparator, executionPlans, null, null, null);
        this.stopWatch.stop("Create Initial Execution Plan", "Pick Best Plan");

        this.stopWatch.start("Create Initial Execution Plan", "Split Stages");
        final ExecutionTaskFlow executionTaskFlow = ExecutionTaskFlow.createFrom(planImplementation);
        final ExecutionPlan executionPlan = ExecutionPlan.createFrom(executionTaskFlow, this.stageSplittingCriterion);
        this.stopWatch.stop("Create Initial Execution Plan", "Split Stages");

        //assert executionPlan.isSane();


        this.stopWatch.stopAll("Create Initial Execution Plan");
        return executionPlan;
    }


    private PlanImplementation pickBestExecutionPlan(Comparator<TimeEstimate> timeEstimateComparator,
                                                     Collection<PlanImplementation> executionPlans,
                                                     ExecutionPlan existingPlan,
                                                     Set<Channel> openChannels,
                                                     Set<ExecutionStage> executedStages) {

        executionPlans.forEach(plan ->
                System.out.printf("Plan (estimated time: %s)\n%s\n", plan.getTimeEstimate(), plan.getOperators())
        );

        return executionPlans.stream()
                .reduce((p1, p2) -> {
                    final TimeEstimate t1 = p1.getTimeEstimate();
                    final TimeEstimate t2 = p2.getTimeEstimate();
                    return timeEstimateComparator.compare(t1, t2) > 0 ? p1 : p2;
                })
                .map(plan -> {
                    final TimeEstimate timeEstimate = plan.getTimeEstimate();
                    this.logger.info("The picked plan's runtime estimate is {}.", timeEstimate);
                    return plan;
                })
                .orElseThrow(() -> new IllegalStateException("Could not find an execution plan."));
    }

    /**
     * Go over the given {@link RheemPlan} and update the cardinalities of data being passed between its
     * {@link Operator}s using the given {@link ExecutionState}.
     */
    private void reestimateCardinalities(ExecutionState executionState) {
        this.cardinalityEstimatorManager.pushCardinalityUpdates(executionState);
    }

    /**
     * Creates a new {@link PlanEnumerator} for the {@link #rheemPlan} and {@link #configuration}.
     */
    private PlanEnumerator createPlanEnumerator() {
        return this.createPlanEnumerator(null, null);
    }

    /**
     * Creates a new {@link PlanEnumerator} for the {@link #rheemPlan} and {@link #configuration}.
     */
    private PlanEnumerator createPlanEnumerator(ExecutionPlan existingPlan, Set<Channel> openChannels) {
        return existingPlan == null ?
                new PlanEnumerator(this.rheemPlan, this.optimizationContext) :
                new PlanEnumerator(this.rheemPlan, this.optimizationContext, existingPlan, openChannels);
    }

    /**
     * Start executing the given {@link ExecutionPlan} with all bells and whistles, such as instrumentation,
     * logging of the plan, and measuring the execution time.
     *
     * @param executionPlan that should be executed
     * @param executionId   an identifier for the current execution
     * @return whether the execution of the {@link ExecutionPlan} is completed
     */
    private boolean execute(ExecutionPlan executionPlan, int executionId) {
        final StopWatch.Round round = this.stopWatch.start(String.format("Execution %d", executionId));

        // Ensure existence of the #crossPlatformExecutor.
        if (this.crossPlatformExecutor == null) {
            final InstrumentationStrategy instrumentation = this.configuration.getInstrumentationStrategyProvider().provide();
            this.crossPlatformExecutor = new CrossPlatformExecutor(this, instrumentation);
        }

        if (this.configuration.getBooleanProperty("rheem.core.optimizer.reoptimize")) {
            this.setUpBreakpoint(executionPlan, round);
        }

        // Log the current executionPlan.
        this.logStages(executionPlan);

        // Trigger the execution.
        final StopWatch.Round executeRound = round.startSubround("Execute");
        boolean isExecutionComplete = this.crossPlatformExecutor.executeUntilBreakpoint(executionPlan);
        executeRound.stop();
        round.stop(true, true);

        // Return.
        return isExecutionComplete;
    }

    /**
     * Sets up a {@link Breakpoint} for an {@link ExecutionPlan}.
     *
     * @param executionPlan for that the {@link Breakpoint} should be set
     * @param round         {@link StopWatch.Round} to be extended for any interesting time measurements
     */
    private void setUpBreakpoint(ExecutionPlan executionPlan, StopWatch.Round round) {

        // Set up appropriate Breakpoints.
        final StopWatch.Round breakpointRound = round.startSubround("Configure Breakpoint");
        FixBreakpoint immediateBreakpoint = new FixBreakpoint();
        final Set<ExecutionStage> completedStages = this.crossPlatformExecutor.getCompletedStages();
        if (completedStages.isEmpty()) {
            executionPlan.getStartingStages().forEach(immediateBreakpoint::breakAfter);
        } else {
            completedStages.stream()
                    .flatMap(stage -> stage.getSuccessors().stream())
                    .filter(stage -> !completedStages.contains(stage))
                    .forEach(immediateBreakpoint::breakAfter);
        }
        this.crossPlatformExecutor.setBreakpoint(new ConjunctiveBreakpoint(
                immediateBreakpoint,
                new CardinalityBreakpoint(this.configuration),
                new NoIterationBreakpoint() // Avoid re-optimization inside of loops.
        ));
        breakpointRound.stop();
    }

    private void logStages(ExecutionPlan executionPlan) {
        if (this.logger.isInfoEnabled()) {

            StringBuilder sb = new StringBuilder();
            Set<ExecutionStage> seenStages = new HashSet<>();
            Queue<ExecutionStage> stagedStages = new LinkedList<>(executionPlan.getStartingStages());
            ExecutionStage nextStage;
            while ((nextStage = stagedStages.poll()) != null) {
                sb.append(nextStage).append(":\n");
                nextStage.getPlanAsString(sb, "* ");
                nextStage.getSuccessors().stream()
                        .filter(seenStages::add)
                        .forEach(stagedStages::add);
            }

            this.logger.info("Current execution plan:\n{}", executionPlan.toExtensiveString());
        }
    }

    /**
     * Injects the cardinalities obtained from {@link Channel} instrumentation, potentially updates the {@link ExecutionPlan}
     * through re-optimization, and collects measured data.
     */
    private void postProcess(ExecutionPlan executionPlan, int executionId) {
        final StopWatch.Round round = this.stopWatch.start(String.format("Post-processing %d", executionId));

        round.startSubround("Reestimate Cardinalities&Time");
        this.reestimateCardinalities(this.crossPlatformExecutor);
        round.stopSubround("Reestimate Cardinalities&Time");

        round.startSubround("Update Execution Plan");
        this.updateExecutionPlan(executionPlan);
        round.stopSubround("Update Execution Plan");

        // Collect any instrumentation results for the future.
        round.startSubround("Store Cardinalities");
        final CardinalityRepository cardinalityRepository = this.rheemContext.getCardinalityRepository();
        cardinalityRepository.storeAll(this.crossPlatformExecutor, this.optimizationContext);
        round.stopSubround("Update Execution Plan");

        round.stop(true, true);
    }

    /**
     * Enumerate possible execution plans from the given {@link RheemPlan} and determine the (seemingly) best one.
     */
    private void updateExecutionPlan(ExecutionPlan executionPlan) {
        // Defines the plan that we want to use in the end.
        final Comparator<TimeEstimate> timeEstimateComparator = this.configuration.getTimeEstimateComparatorProvider().provide();

        // Find and copy the open Channels.
        final Set<ExecutionStage> completedStages = this.crossPlatformExecutor.getCompletedStages();
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
        final PlanEnumerator planEnumerator = this.createPlanEnumerator(executionPlan, openChannels);
        final PlanEnumeration comprehensiveEnumeration = planEnumerator.enumerate(true);
        final Collection<PlanImplementation> executionPlans = comprehensiveEnumeration.getPlanImplementations();
        this.logger.info("Enumerated {} plans.", executionPlans.size());
        for (PlanImplementation planImplementation : executionPlans) {
            this.logger.debug("Plan with operators: {}", planImplementation.getOperators());
        }

        // Pick an execution plan.
        // Make sure that an execution plan can be created.
        final PlanImplementation planImplementation = this.pickBestExecutionPlan(timeEstimateComparator, executionPlans, executionPlan,
                openChannels, completedStages);

        ExecutionTaskFlow executionTaskFlow = ExecutionTaskFlow.recreateFrom(
                planImplementation, executionPlan, openChannels, completedStages
        );
        final ExecutionPlan executionPlanExpansion = ExecutionPlan.createFrom(executionTaskFlow, this.stageSplittingCriterion);
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

    public Set<String> getUdfJarPaths() {
        return this.udfJarPaths;
    }

    /**
     * Provide the {@link CrossPlatformExecutor} used during the execution of this instance.
     *
     * @return the {@link CrossPlatformExecutor} or {@code null} if there is none allocated
     */
    public CrossPlatformExecutor getCrossPlatformExecutor() {
        return this.crossPlatformExecutor;
    }

    public OptimizationContext getOptimizationContext() {
        return optimizationContext;
    }
}
