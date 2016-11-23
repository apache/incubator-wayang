package org.qcri.rheem.profiler.log;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.optimizer.ProbabilisticIntervalEstimate;
import org.qcri.rheem.core.optimizer.costs.TimeEstimate;
import org.qcri.rheem.core.platform.AtomicExecutionGroup;
import org.qcri.rheem.core.platform.PartialExecution;
import org.qcri.rheem.core.profiling.ExecutionLog;
import org.qcri.rheem.core.util.Formats;
import org.qcri.rheem.java.platform.JavaPlatform;
import org.qcri.rheem.spark.platform.SparkPlatform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Evaluates a {@link Configuration} on a {@link ExecutionLog}.
 */
public class LogEvaluator {

    private static final Logger logger = LoggerFactory.getLogger(LogEvaluator.class);

    private final Configuration configuration;

    private final Collection<PartialExecution> partialExecutions;

    private final Collection<Predicate<PartialExecution>> filters = new LinkedList<>();

    private Comparator<PartialExecution> sortCriterion;

    private boolean isSortAscending = true;

    public LogEvaluator(Configuration configuration) {
        // Initialize platforms - otherwise, we get some errors.
        JavaPlatform.getInstance();
        SparkPlatform.getInstance();

        this.configuration = configuration;
        this.partialExecutions = loadLog(configuration);

        // Print some general statistics.
        this.modifySorting("sort rel desc".split(" "));
        this.printStatistics();
    }

    private static Collection<PartialExecution> loadLog(Configuration configuration) {
        try (ExecutionLog executionLog = ExecutionLog.open(configuration)) {
            return executionLog.stream().collect(Collectors.toList());
        } catch (Exception e) {
            throw new RheemException("Could not evaluate execution log.", e);
        }
    }

    private void runUserLoop() throws IOException {
        String input;
        BufferedReader commandLine = new BufferedReader(new InputStreamReader(System.in));
        while ((input = commandLine.readLine()) != null) {
            final String[] tokens = input.split("\\s+");
            switch (tokens[0]) {
                case "print":
                    this.printPartialExecutions(tokens);
                    break;
                case "stats":
                    this.printStatistics();
                    break;
                case "filter":
                    this.modifyFilters(tokens);
                    break;
                case "sort":
                    this.modifySorting(tokens);
                    break;
                case "exit":
                    return;
                default:
                    System.out.println("Unknown command.");
            }
        }
    }

    private void printPartialExecutions(String[] commandLine) {
        Stream<PartialExecution> stream = createPartialExecutionStream();
        if (commandLine.length >= 2) {
            stream = stream.limit(Long.parseLong(commandLine[1]));
        }
        stream.forEach(this::print);
    }

    private void print(PartialExecution pe) {
        System.out.printf("Partial execution with %d execution groups:\n", pe.getAtomicExecutionGroups().size());
        System.out.printf("> Measured execution time: %s\n", Formats.formatDuration(pe.getMeasuredExecutionTime(), true));
        System.out.printf("> Estimated execution time: %s\n", pe.getOverallTimeEstimate(this.configuration));
        System.out.printf("> Delta: %s\n", pe.getOverallTimeEstimate(this.configuration).plus(-pe.getMeasuredExecutionTime()));
        for (AtomicExecutionGroup atomicExecutionGroup : pe.getAtomicExecutionGroups()) {
            System.out.printf("--> %s: %s\n", atomicExecutionGroup, atomicExecutionGroup.estimateExecutionTime());
        }
        System.out.println();
    }

    private void printStatistics() {
        // Print some general statistics.
        final TimeEstimate overallTimeEstimate = this.createPartialExecutionStream()
                .map(partialExecution -> partialExecution.getOverallTimeEstimate(this.configuration))
                .reduce(TimeEstimate.ZERO, TimeEstimate::plus);
        final long overallMeasuredTime = this.createPartialExecutionStream()
                .map(PartialExecution::getMeasuredExecutionTime)
                .reduce(0L, (a, b) -> a + b);
        System.out.printf("Found %d partial executions.\n", this.createPartialExecutionStream().count());
        System.out.printf("> Measured execution time: %s\n", Formats.formatDuration(overallMeasuredTime, true));
        System.out.printf("> Estimated execution time: %s\n", overallTimeEstimate);
        System.out.println();
    }

    private void modifyFilters(String[] commandLine) {
        if (commandLine.length == 1) {
            System.out.println("filter <name|platform|operator> <regex>");
            System.out.println("filter clear");
            return;
        }
        Predicate<PartialExecution> predicate;
        switch (commandLine[1]) {
            case "clear":
                this.filters.clear();
                return;
            case "name":
                System.out.println("Not supported.");
//                predicate = pe -> pe.getOperatorContexts().stream()
//                        .map(operatorContext -> operatorContext.getOperator().getName())
//                        .filter(Objects::nonNull)
//                        .anyMatch(name -> matchSubstring(name, commandLine[2]));
                break;
            case "operator":
                System.out.println("Not supported.");
//                predicate = pe -> pe.getOperatorContexts().stream()
//                        .map(operatorContext -> operatorContext.getOperator().getClass().getSimpleName())
//                        .anyMatch(name -> matchSubstring(name, commandLine[2]));
                break;
            case "platform":
                System.out.println("Not supported.");
//                predicate = pe -> pe.getOperatorContexts().stream()
//                        .map(operatorContext -> ((ExecutionOperator) operatorContext.getOperator()).getPlatform())
//                        .anyMatch(platform -> matchSubstring(platform.getName(), commandLine[2]));
                break;
            default:
                System.out.println("Unknown filter type.");
                return;
        }
//        this.filters.add(predicate);
    }

    private void modifySorting(String[] commandLine) {
        if (commandLine.length == 1) {
            System.out.println("sort <est|run|delta> [asc|desc]");
            System.out.println("sort clear");
            return;
        }
        final Comparator<TimeEstimate> timeEstimateComparator = ProbabilisticIntervalEstimate.expectationValueComparator();
        switch (commandLine[1]) {
            case "clear":
                this.sortCriterion = null;
                return;
            case "est":
                this.sortCriterion = (a, b) -> timeEstimateComparator.compare(
                        a.getOverallTimeEstimate(this.configuration),
                        b.getOverallTimeEstimate(this.configuration)
                );
                break;
            case "run":
                this.sortCriterion = (a, b) -> Long.compare(a.getMeasuredExecutionTime(), b.getMeasuredExecutionTime());
                break;
            case "abs":
                this.sortCriterion = (a, b) -> timeEstimateComparator.compare(
                        a.getOverallTimeEstimate(this.configuration).plus(-a.getMeasuredExecutionTime()),
                        b.getOverallTimeEstimate(this.configuration).plus(-a.getMeasuredExecutionTime())
                );
                break;
            case "rel":
                this.sortCriterion = (a, b) -> timeEstimateComparator.compare(
                        a.getOverallTimeEstimate(this.configuration).times(1d / a.getMeasuredExecutionTime()),
                        b.getOverallTimeEstimate(this.configuration).times(1d / -a.getMeasuredExecutionTime())
                );
                break;
            default:
                System.out.println("Unknown filter type.");
                return;
        }
        this.isSortAscending = commandLine.length < 3 || commandLine[2].equalsIgnoreCase("asc");
    }

    private static boolean matchSubstring(String inputString, String regex) {
        return inputString.toLowerCase().matches(".*" + regex + ".*");
    }

    private Stream<PartialExecution> createPartialExecutionStream() {
        Stream<PartialExecution> stream = this.partialExecutions.stream();
        for (Predicate<PartialExecution> filter : this.filters) {
            stream = stream.filter(filter);
        }
        if (this.sortCriterion != null) {
            stream = stream.sorted(this.isSortAscending ?
                    this.sortCriterion :
                    (a, b) -> -this.sortCriterion.compare(a, b));
        }
        return stream;
    }


    public static void main(String[] args) throws IOException {
        LogEvaluator logEvaluator = new LogEvaluator(new Configuration());
        logEvaluator.runUserLoop();
    }

}
