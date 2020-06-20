package org.qcri.rheem.core.api;

import de.hpi.isg.profiledb.instrumentation.StopWatch;
import de.hpi.isg.profiledb.store.model.Experiment;
import de.hpi.isg.profiledb.store.model.TimeMeasurement;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.mapping.PlanTransformation;
import org.qcri.rheem.core.monitor.DisabledMonitor;
import org.qcri.rheem.core.monitor.FileMonitor;
import org.qcri.rheem.core.monitor.Monitor;
import org.qcri.rheem.core.optimizer.DefaultOptimizationContext;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.ProbabilisticDoubleInterval;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimatorManager;
import org.qcri.rheem.core.optimizer.costs.TimeEstimate;
import org.qcri.rheem.core.optimizer.costs.TimeToCostConverter;
import org.qcri.rheem.core.optimizer.enumeration.ExecutionTaskFlow;
import org.qcri.rheem.core.optimizer.enumeration.PlanEnumeration;
import org.qcri.rheem.core.optimizer.enumeration.PlanEnumerator;
import org.qcri.rheem.core.optimizer.enumeration.PlanImplementation;
import org.qcri.rheem.core.optimizer.enumeration.StageAssignmentTraversal;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionPlan;
import org.qcri.rheem.core.plan.executionplan.ExecutionStage;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.plan.rheemplan.PlanMetrics;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.platform.AtomicExecutionGroup;
import org.qcri.rheem.core.platform.Breakpoint;
import org.qcri.rheem.core.platform.CardinalityBreakpoint;
import org.qcri.rheem.core.platform.ConjunctiveBreakpoint;
import org.qcri.rheem.core.platform.CrossPlatformExecutor;
import org.qcri.rheem.core.platform.ExecutionState;
import org.qcri.rheem.core.platform.FixBreakpoint;
import org.qcri.rheem.core.platform.NoIterationBreakpoint;
import org.qcri.rheem.core.platform.PartialExecution;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.profiling.CardinalityRepository;
import org.qcri.rheem.core.profiling.CostMeasurement;
import org.qcri.rheem.core.profiling.ExecutionLog;
import org.qcri.rheem.core.profiling.ExecutionPlanMeasurement;
import org.qcri.rheem.core.profiling.InstrumentationStrategy;
import org.qcri.rheem.core.profiling.PartialExecutionMeasurement;
import org.qcri.rheem.core.util.Formats;
import org.qcri.rheem.core.util.OneTimeExecutable;
import org.qcri.rheem.core.util.ReflectionUtils;
import org.qcri.rheem.core.util.RheemCollections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
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
    private DefaultOptimizationContext optimizationContext;

    /**
     * General purpose cache.
     */
    private Map<String, Object> cache = new HashMap<>();

    /**
     * Executes the optimized {@link ExecutionPlan}.
     */
    private CrossPlatformExecutor crossPlatformExecutor;

    /**
     * Manages the {@link CardinalityEstimate}s for the {@link #rheemPlan}.
     */
    private CardinalityEstimatorManager cardinalityEstimatorManager;

    /**
     * Collects metadata w.r.t. the processing of this instance.
     */
    private final Experiment experiment;

    /**
     * {@link StopWatch} to measure some key figures for the {@link #experiment}.
     */
    private final StopWatch stopWatch;

    /**
     * {@link TimeMeasurement}s for the optimization and the execution phases.
     */
    private final TimeMeasurement optimizationRound, executionRound;

    /**
     * Collects the {@link TimeEstimate}s of all (partially) executed {@link PlanImplementation}s.
     */
    private List<TimeEstimate> timeEstimates = new LinkedList<>();

    /**
     * Collects the cost estimates of all (partially) executed {@link PlanImplementation}s.
     */
    private List<ProbabilisticDoubleInterval> costEstimates = new LinkedList<>();

    /**
     * JAR files that are needed to execute the UDFs.
     */
    private final Set<String> udfJarPaths = new HashSet<>();

    private Monitor monitor;

    /**
     * Name for this instance.
     */
    private final String name;

    /**
     * <i>Currently not used.</i>
     */
    private final StageAssignmentTraversal.StageSplittingCriterion stageSplittingCriterion =
            (producerTask, channel, consumerTask) -> false;

    /**
     * The {@link PlanImplementation} that is being executed.
     */
    private PlanImplementation planImplementation;

    /**
     * Controls at which {@link CardinalityEstimate}s the execution should be interrupted.
     */
    private final CardinalityBreakpoint cardinalityBreakpoint;

    private final boolean isProactiveReoptimization;

    /**
     * Creates a new instance.
     *
     * @param name       name for this instance or {@code null} if a default name should be picked
     * @param experiment an {@link Experiment} for that profiling entries will be created
     * @param udfJars    paths to JAR files needed to run the UDFs (see {@link ReflectionUtils#getDeclaringJar(Class)})
     */
    Job(RheemContext rheemContext, String name, Monitor monitor, RheemPlan rheemPlan, Experiment experiment, String... udfJars) {
        this.rheemContext = rheemContext;
        this.name = name == null ? "Rheem app" : name;
        this.configuration = this.rheemContext.getConfiguration().fork(this.name);
        this.rheemPlan = rheemPlan;
        for (String udfJar : udfJars) {
            this.addUdfJar(udfJar);
        }

        // Prepare re-optimization.
        if (this.configuration.getBooleanProperty("rheem.core.optimizer.reoptimize")) {
            this.cardinalityBreakpoint = new CardinalityBreakpoint(this.configuration);
            this.isProactiveReoptimization =
                    this.configuration.getBooleanProperty("rheem.core.optimizer.reoptimize.proactive", false);
        } else {
            this.cardinalityBreakpoint = null;
            this.isProactiveReoptimization = false;
        }

        // Prepare instrumentation.
        this.experiment = experiment;
        this.stopWatch = new StopWatch(experiment);
        this.optimizationRound = this.stopWatch.getOrCreateRound("Optimization");
        this.executionRound = this.stopWatch.getOrCreateRound("Execution");

        // Configure job monitor.
        if (Monitor.isEnabled(this.configuration)) {
            this.monitor = monitor == null ? new FileMonitor() : monitor;
        } else {
            this.monitor = new DisabledMonitor();
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
     *
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

    public ExecutionPlan buildInitialExecutionPlan() throws RheemException {
        this.prepareRheemPlan();
        this.estimateKeyFigures();

        // Get initial execution plan.
        ExecutionPlan executionPlan = this.createInitialExecutionPlan();
        return executionPlan;
    }

    // TODO: Move outside of Job class
    public void reportProgress(String opName, Integer progress) {
        HashMap<String, Integer> partialProgress = new HashMap<>();
        partialProgress.put(opName, progress);
        try {
            this.monitor.updateProgress(partialProgress);
        } catch (IOException e) {
            e.printStackTrace();
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
            this.optimizationRound.start();
            this.prepareRheemPlan();

            // Estimate cardinalities and execution times for the #rheemPlan.
            this.estimateKeyFigures();

            // Get an execution plan.
            int executionId = 0;
            ExecutionPlan executionPlan = this.createInitialExecutionPlan();
            this.optimizationRound.stop();
            if (this.experiment != null) {
                this.experiment.addMeasurement(ExecutionPlanMeasurement.capture(
                        executionPlan,
                        String.format("execution-plan-%d", executionId)
                ));
            }

            // TODO: generate run ID. For now we fix this because we can't handle multiple jobs, neither in montoring nor execution.
            String runId = "1";
            try {
                monitor.initialize(this.configuration, runId, executionPlan.toJsonList());
            } catch (Exception e) {
                this.logger.warn("Failed to initialize monitor: {}", e);
            }


            // Take care of the execution.
            while (!this.execute(executionPlan, executionId)) {
                this.optimizationRound.start();
                if (this.postProcess(executionPlan, executionId)) {
                    executionId++;
                    if (this.experiment != null) {
                        this.experiment.addMeasurement(ExecutionPlanMeasurement.capture(
                                executionPlan,
                                String.format("execution-plan-%d", executionId)
                        ));
                    }
                }
                this.optimizationRound.stop();
            }

            this.stopWatch.start("Post-processing");
            if (this.configuration.getBooleanProperty("rheem.core.log.enabled")) {
                this.logExecution();
            }
        } catch (RheemException e) {
            throw e;
        } catch (Throwable t) {
            throw new RheemException("Job execution failed.", t);
        } finally {
            this.stopWatch.stopAll();
            this.stopWatch.start("Post-processing", "Release Resources");
            this.releaseResources();
            this.stopWatch.stop("Post-processing");
            this.logger.info("StopWatch results:\n{}", this.stopWatch.toPrettyString());
        }
    }

    /**
     * Prepares the {@link #rheemPlan}: prunes unused {@link Operator}s, isolates loops, and applies all available
     * {@link PlanTransformation}s.
     */
    private void prepareRheemPlan() {
        this.logger.info("Preparing plan...");

        // Prepare the RheemPlan for the optimization.
        this.optimizationRound.start("Prepare", "Prune&Isolate");
        this.rheemPlan.prepare();
        this.optimizationRound.stop("Prepare", "Prune&Isolate");

        // Apply the mappings to the plan to form a hyperplan.
        this.optimizationRound.start("Prepare", "Transformations");
        final Collection<PlanTransformation> transformations = this.gatherTransformations();
        this.rheemPlan.applyTransformations(transformations);
        this.optimizationRound.stop("Prepare", "Transformations");

        this.optimizationRound.start("Prepare", "Sanity");
        assert this.rheemPlan.isSane();
        this.optimizationRound.stop("Prepare", "Sanity");

        this.optimizationRound.stop("Prepare");
    }

    /**
     * Gather all available {@link PlanTransformation}s from the {@link #configuration}.
     */
    private Collection<PlanTransformation> gatherTransformations() {
        final Set<Platform> platforms = RheemCollections.asSet(this.configuration.getPlatformProvider().provideAll());
        return this.configuration.getMappingProvider().provideAll().stream()
                .flatMap(mapping -> mapping.getTransformations().stream())
                .filter(t -> t.getTargetPlatforms().isEmpty() || platforms.containsAll(t.getTargetPlatforms()))
                .collect(Collectors.toList());
    }


    /**
     * Go over the given {@link RheemPlan} and estimate the cardinalities of data being passed between its
     * {@link Operator}s and the execution profile and time of {@link ExecutionOperator}s.
     */
    private void estimateKeyFigures() {
        this.logger.info("Estimating cardinalities and execution load...");

        this.optimizationRound.start("Cardinality&Load Estimation");
        if (this.cardinalityEstimatorManager == null) {
            this.optimizationRound.start("Cardinality&Load Estimation", "Create OptimizationContext");
            this.optimizationContext = DefaultOptimizationContext.createFrom(this);
            this.optimizationRound.stop("Cardinality&Load Estimation", "Create OptimizationContext");

            this.optimizationRound.start("Cardinality&Load Estimation", "Create CardinalityEstimationManager");
            this.cardinalityEstimatorManager = new CardinalityEstimatorManager(
                    this.rheemPlan, this.optimizationContext, this.configuration);
            this.optimizationRound.stop("Cardinality&Load Estimation", "Create CardinalityEstimationManager");
        }

        this.optimizationRound.start("Cardinality&Load Estimation", "Push Estimation");
        this.cardinalityEstimatorManager.pushCardinalities();
        this.optimizationRound.stop("Cardinality&Load Estimation", "Push Estimation");

        this.optimizationRound.stop("Cardinality&Load Estimation");
    }


    /**
     * Determine a good/the best execution plan from a given {@link RheemPlan}.
     */
    private ExecutionPlan createInitialExecutionPlan() {
        this.logger.info("Enumerating execution plans...");

        this.optimizationRound.start("Create Initial Execution Plan");

        // Enumerate all possible plan.
        final PlanEnumerator planEnumerator = this.createPlanEnumerator();

        final TimeMeasurement enumerateMeasurment = this.optimizationRound.start("Create Initial Execution Plan", "Enumerate");
        planEnumerator.setTimeMeasurement(enumerateMeasurment);
        final PlanEnumeration comprehensiveEnumeration = planEnumerator.enumerate(true);
        planEnumerator.setTimeMeasurement(null);
        this.optimizationRound.stop("Create Initial Execution Plan", "Enumerate");

        final Collection<PlanImplementation> executionPlans = comprehensiveEnumeration.getPlanImplementations();
        this.logger.debug("Enumerated {} plans.", executionPlans.size());
        for (PlanImplementation planImplementation : executionPlans) {
            this.logger.debug("Plan with operators: {}", planImplementation.getOperators());
        }

        // Pick an execution plan.
        // Make sure that an execution plan can be created.
        this.optimizationRound.start("Create Initial Execution Plan", "Pick Best Plan");
        this.pickBestExecutionPlan(executionPlans, null, null, null);
        this.timeEstimates.add(planImplementation.getTimeEstimate());
        this.costEstimates.add(planImplementation.getCostEstimate());
        this.optimizationRound.stop("Create Initial Execution Plan", "Pick Best Plan");

        this.logger.info("Compiling execution plan...");
        this.optimizationRound.start("Create Initial Execution Plan", "Split Stages");
        final ExecutionTaskFlow executionTaskFlow = ExecutionTaskFlow.createFrom(this.planImplementation);
        final ExecutionPlan executionPlan = ExecutionPlan.createFrom(executionTaskFlow, this.stageSplittingCriterion);
        this.optimizationRound.stop("Create Initial Execution Plan", "Split Stages");

        this.planImplementation.mergeJunctionOptimizationContexts();

        this.planImplementation.logTimeEstimates();

        //assert executionPlan.isSane();


        this.optimizationRound.stop("Create Initial Execution Plan");
        return executionPlan;
    }


    private PlanImplementation pickBestExecutionPlan(Collection<PlanImplementation> executionPlans,
                                                     ExecutionPlan existingPlan,
                                                     Set<Channel> openChannels,
                                                     Set<ExecutionStage> executedStages) {

        final PlanImplementation bestPlanImplementation = executionPlans.stream()
                .reduce((p1, p2) -> {
                    final double t1 = p1.getSquashedCostEstimate();
                    final double t2 = p2.getSquashedCostEstimate();
                    return t1 < t2 ? p1 : p2;
                })
                .orElseThrow(() -> new RheemException("Could not find an execution plan."));
        this.logger.info("Picked {} as best plan.", bestPlanImplementation);
        return this.planImplementation = bestPlanImplementation;
    }

    /**
     * Go over the given {@link RheemPlan} and update the cardinalities of data being passed between its
     * {@link Operator}s using the given {@link ExecutionState}.
     *
     * @return whether any cardinalities have been updated
     */
    private boolean reestimateCardinalities(ExecutionState executionState) {
        return this.cardinalityEstimatorManager.pushCardinalityUpdates(executionState, this.planImplementation);
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
        final TimeMeasurement currentExecutionRound = this.executionRound.start(String.format("Execution %d", executionId));

        // Ensure existence of the #crossPlatformExecutor.
        if (this.crossPlatformExecutor == null) {
            final InstrumentationStrategy instrumentation = this.configuration.getInstrumentationStrategyProvider().provide();
            this.crossPlatformExecutor = new CrossPlatformExecutor(this, instrumentation);
        }

        if (this.configuration.getOptionalBooleanProperty("rheem.core.debug.skipexecution").orElse(false)) {
            return true;
        }
        if (this.configuration.getBooleanProperty("rheem.core.optimizer.reoptimize")) {
            this.setUpBreakpoint(executionPlan, currentExecutionRound);
        }

        // Log the current executionPlan.
        this.logStages(executionPlan);

        // Trigger the execution.
        currentExecutionRound.start("Execute");
        boolean isExecutionComplete = this.crossPlatformExecutor.executeUntilBreakpoint(
                executionPlan, this.optimizationContext
        );
        executionRound.stop();

        // Return.
        return isExecutionComplete;
    }

    /**
     * Sets up a {@link Breakpoint} for an {@link ExecutionPlan}.
     *
     * @param executionPlan for that the {@link Breakpoint} should be set
     * @param round         {@link TimeMeasurement} to be extended for any interesting time measurements
     */
    private void setUpBreakpoint(ExecutionPlan executionPlan, TimeMeasurement round) {

        // Set up appropriate Breakpoints.
        final TimeMeasurement breakpointRound = round.start("Configure Breakpoint");
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
                this.cardinalityBreakpoint,
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
     *
     * @return whether the {@link ExecutionPlan} has been re-optimized
     */
    private boolean postProcess(ExecutionPlan executionPlan, int executionId) {
        if (this.crossPlatformExecutor.isVetoingPlanChanges()) {
            this.logger.info("The cross-platform executor is currently not allowing re-optimization.");
            return false;
        }

        final TimeMeasurement round = this.optimizationRound.start(String.format("Post-processing %d", executionId));

        round.start("Reestimate Cardinalities&Time");
        boolean isCardinalitiesUpdated = this.reestimateCardinalities(this.crossPlatformExecutor);
        round.stop("Reestimate Cardinalities&Time");

        round.start("Update Execution Plan");
        if (isCardinalitiesUpdated) {
            this.logger.info("Re-optimizing execution plan.");
            this.updateExecutionPlan(executionPlan);
        } else {
            this.logger.info("Skipping re-optimization: no new insights on cardinalities.");
            this.timeEstimates.add(this.timeEstimates.get(this.timeEstimates.size() - 1));
            this.costEstimates.add(this.costEstimates.get(this.costEstimates.size() - 1));

        }
        round.stop("Update Execution Plan");

        round.stop();

        return true;
    }

    /**
     * Enumerate possible execution plans from the given {@link RheemPlan} and determine the (seemingly) best one.
     */
    private void updateExecutionPlan(ExecutionPlan executionPlan) {
        // Defines the plan that we want to use in the end.
        // Find and copy the open Channels.
        final Set<ExecutionStage> completedStages = this.crossPlatformExecutor.getCompletedStages();
        final Set<ExecutionTask> completedTasks = completedStages.stream()
                .flatMap(stage -> stage.getAllTasks().stream())
                .collect(Collectors.toSet());

        // Find Channels that have yet to be consumed by unexecuted ExecutionTasks and scrap unexecuted bits of the plan.
        final Set<Channel> openChannels = executionPlan.retain(completedStages);

        // Enumerate all possible plan.
        final PlanEnumerator planEnumerator = this.createPlanEnumerator(executionPlan, openChannels);
        final PlanEnumeration comprehensiveEnumeration = planEnumerator.enumerate(true);
        final Collection<PlanImplementation> executionPlans = comprehensiveEnumeration.getPlanImplementations();
        this.logger.debug("Enumerated {} plans.", executionPlans.size());
        for (PlanImplementation planImplementation : executionPlans) {
            this.logger.debug("Plan with operators: {}", planImplementation.getOperators());
        }

        // Pick an execution plan.
        // Make sure that an execution plan can be created.
        this.pickBestExecutionPlan(executionPlans, executionPlan, openChannels, completedStages);
        this.timeEstimates.add(this.planImplementation.getTimeEstimate());
        this.costEstimates.add(this.planImplementation.getCostEstimate());

        ExecutionTaskFlow executionTaskFlow = ExecutionTaskFlow.recreateFrom(
                planImplementation, executionPlan, openChannels, completedStages
        );
        final ExecutionPlan executionPlanExpansion = ExecutionPlan.createFrom(executionTaskFlow, this.stageSplittingCriterion);
        executionPlan.expand(executionPlanExpansion);

        this.planImplementation.mergeJunctionOptimizationContexts();

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

    private void logExecution() {
        this.stopWatch.start("Post-processing", "Log measurements");

        // For the last time, update the cardinalities and store them.
        this.reestimateCardinalities(this.crossPlatformExecutor);
        final CardinalityRepository cardinalityRepository = this.rheemContext.getCardinalityRepository();
        cardinalityRepository.storeAll(this.crossPlatformExecutor, this.optimizationContext);

        // Execution times.
        final Collection<PartialExecution> partialExecutions = this.crossPlatformExecutor.getPartialExecutions();

        // Add the execution times to the experiment.
        int nextPartialExecutionMeasurementId = 0;
        for (PartialExecution partialExecution : partialExecutions) {
            if (this.logger.isDebugEnabled()) {
                for (AtomicExecutionGroup atomicExecutionGroup : partialExecution.getAtomicExecutionGroups()) {
                    if (!(atomicExecutionGroup.getEstimationContext() instanceof OptimizationContext.OperatorContext)) {
                        continue;
                    }
                    OptimizationContext.OperatorContext operatorContext =
                            (OptimizationContext.OperatorContext) atomicExecutionGroup.getEstimationContext();

                    for (CardinalityEstimate cardinality : operatorContext.getInputCardinalities()) {
                        if (cardinality != null && !cardinality.isExact()) {
                            this.logger.debug(
                                    "Inexact input cardinality estimate {} for {}.",
                                    cardinality, operatorContext.getOperator()
                            );
                        }
                    }
                    for (CardinalityEstimate cardinality : operatorContext.getOutputCardinalities()) {
                        if (cardinality != null && !cardinality.isExact()) {
                            this.logger.debug(
                                    "Inexact output cardinality estimate {} for {}.",
                                    cardinality, operatorContext.getOperator()
                            );
                        }
                    }
                }
            }
            String id = String.format("par-ex-%03d", nextPartialExecutionMeasurementId++);
            final PartialExecutionMeasurement measurement = new PartialExecutionMeasurement(id, partialExecution, this.configuration);
            this.experiment.addMeasurement(measurement);
        }

        // Feed the execution log.
        try (ExecutionLog executionLog = ExecutionLog.open(this.configuration)) {
            executionLog.storeAll(partialExecutions);
        } catch (Exception e) {
            this.logger.error("Storing partial executions failed.", e);
        }
        this.optimizationRound.stop("Post-processing", "Log measurements");

        // Log the execution time.
        long effectiveExecutionMillis = partialExecutions.stream()
                .map(PartialExecution::getMeasuredExecutionTime)
                .reduce(0L, (a, b) -> a + b);
        long measuredExecutionMillis = this.executionRound.getMillis();
        this.logger.info(
                "Accumulated execution time: {} (effective: {}, overhead: {})",
                Formats.formatDuration(measuredExecutionMillis, true),
                Formats.formatDuration(effectiveExecutionMillis, true),
                Formats.formatDuration(measuredExecutionMillis - effectiveExecutionMillis, true)
        );
        int i = 1;
        for (TimeEstimate timeEstimate : this.timeEstimates) {
            this.logger.info("Estimated execution time (plan {}): {}", i, timeEstimate);
            TimeMeasurement lowerEstimate = new TimeMeasurement(String.format("Estimate %d (lower)", i));
            lowerEstimate.setMillis(timeEstimate.getLowerEstimate());
            this.stopWatch.getExperiment().addMeasurement(lowerEstimate);
            TimeMeasurement upperEstimate = new TimeMeasurement(String.format("Estimate %d (upper)", i));
            upperEstimate.setMillis(timeEstimate.getUpperEstimate());
            this.stopWatch.getExperiment().addMeasurement(upperEstimate);
            i++;
        }

        // Log the cost settings.
        final Collection<Platform> consideredPlatforms = this.configuration.getPlatformProvider().provideAll();
        for (Platform consideredPlatform : consideredPlatforms) {
            final TimeToCostConverter timeToCostConverter = this.configuration
                    .getTimeToCostConverterProvider()
                    .provideFor(consideredPlatform);
            this.experiment.getSubject().addConfiguration(
                    String.format("Costs per ms (%s)", consideredPlatform.getName()),
                    timeToCostConverter.getCostsPerMillisecond()
            );
            this.experiment.getSubject().addConfiguration(
                    String.format("Fix costs (%s)", consideredPlatform.getName()),
                    timeToCostConverter.getFixCosts()
            );
        }


        // Log the execution costs.
        double fixCosts = partialExecutions.stream()
                .flatMap(partialExecution -> partialExecution.getInvolvedPlatforms().stream())
                .map(platform -> this.configuration.getTimeToCostConverterProvider().provideFor(platform).getFixCosts())
                .reduce(0d, (a, b) -> a + b);
        double effectiveLowerCosts = fixCosts + partialExecutions.stream()
                .map(PartialExecution::getMeasuredLowerCost)
                .reduce(0d, (a, b) -> a + b);
        double effectiveUpperCosts = fixCosts + partialExecutions.stream()
                .map(PartialExecution::getMeasuredUpperCost)
                .reduce(0d, (a, b) -> a + b);
        this.logger.info("Accumulated costs: {} .. {}",
                String.format("%,.2f", effectiveLowerCosts),
                String.format("%,.2f", effectiveUpperCosts)
        );
        this.experiment.addMeasurement(
                new CostMeasurement("Measured cost", effectiveLowerCosts, effectiveUpperCosts, 1d)
        );
        i = 1;
        for (ProbabilisticDoubleInterval costEstimate : this.costEstimates) {
            this.logger.info("Estimated costs (plan {}): {}", i, costEstimate);
            this.experiment.addMeasurement(new CostMeasurement(
                    String.format("Estimated costs (%d)", i),
                    costEstimate.getLowerEstimate(),
                    costEstimate.getUpperEstimate(),
                    costEstimate.getCorrectnessProbability()
            ));
            i++;
        }

        // Log some plan metrics.
        final PlanMetrics planMetrics = PlanMetrics.createFor(this.rheemPlan, "Plan Metrics");
        this.logger.info("Plan metrics: {} virtual operators, {} execution operators, {} alternatives, {} combinations",
                planMetrics.getNumVirtualOperators(),
                planMetrics.getNumExecutionOperators(),
                planMetrics.getNumAlternatives(),
                planMetrics.getNumCombinations()
        );
        this.experiment.addMeasurement(planMetrics);
    }

    /**
     * Whether a {@link Breakpoint} is requested for the given {@link OutputSlot}.
     *
     * @param output          the {@link OutputSlot}
     * @param operatorContext the {@link OptimizationContext.OperatorContext} for the {@link OutputSlot} owner
     * @return whether a {@link Breakpoint} is requested
     */
    public boolean isRequestBreakpointFor(OutputSlot<?> output, OptimizationContext.OperatorContext operatorContext) {
        return this.isProactiveReoptimization
                && output.getOwner().getInnermostLoop() == null
                && this.cardinalityBreakpoint != null
                && !this.cardinalityBreakpoint.approves(operatorContext.getOutputCardinality(output.getIndex())
        );
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

    public DefaultOptimizationContext getOptimizationContext() {
        return this.optimizationContext;
    }

    /**
     * Retrieves the name of this instance.
     *
     * @return the name
     */
    public String getName() {
        return this.name;
    }

    @Override
    public String toString() {
        return String.format("%s[%s]", this.getClass().getSimpleName(), this.name);
    }

    /**
     * Provide the {@link Experiment} being recorded with the execution of this instance.
     *
     * @return the {@link Experiment}
     */
    public Experiment getExperiment() {
        return this.experiment;
    }

    /**
     * Provide the {@link RheemPlan} executed by this instance.
     *
     * @return the {@link RheemPlan}
     */
    public RheemPlan getRheemPlan() {
        return this.rheemPlan;
    }

    /**
     * Provide the {@link StopWatch} that is used to instrument the execution of this instance.
     *
     * @return the {@link StopWatch}
     */
    public StopWatch getStopWatch() {
        return this.stopWatch;
    }

    /**
     * Provides a general-purpose cache. Can be used to communicate job-global information.
     *
     * @return the cache
     */
    public Map<String, Object> getCache() {
        return this.cache;
    }
}
