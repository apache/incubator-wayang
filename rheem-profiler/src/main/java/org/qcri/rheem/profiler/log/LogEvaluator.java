package org.qcri.rheem.profiler.log;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.costs.TimeEstimate;
import org.qcri.rheem.core.profiling.ExecutionLog;
import org.qcri.rheem.java.JavaPlatform;
import org.qcri.rheem.spark.platform.SparkPlatform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Evaluates a {@link Configuration} on a {@link ExecutionLog}.
 */
public class LogEvaluator {

    private static final Logger logger = LoggerFactory.getLogger(LogEvaluator.class);

    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        JavaPlatform.getInstance();
        SparkPlatform.getInstance();
        EvaluationContext evaluationContext = new EvaluationContext();
        try (ExecutionLog executionLog = ExecutionLog.open(configuration)) {
            executionLog.stream(configuration).forEach(partialExecution -> {
                        evaluationContext.measuredTimes.add(partialExecution.getMeasuredExecutionTime());
                        evaluationContext.timeEstimates.add(partialExecution.getOverallTimeEstimate());
                        System.out.printf("Execution of %s:\n", partialExecution.getOperatorContexts().stream()
                                .map(OptimizationContext.OperatorContext::getOperator)
                                .collect(Collectors.toList()));
                        System.out.printf("Measured %s; estimated %s.\n",
                                partialExecution.getMeasuredExecutionTime(),
                                partialExecution.getOverallTimeEstimate());
                    }
            );
        } catch (Exception e) {
            logger.error("Could not evaluate execution log.", e);
        }

        final TimeEstimate overallTimeEstimate = evaluationContext.timeEstimates.stream().reduce(TimeEstimate.ZERO, TimeEstimate::plus);
        final long overallMeasuredTime = evaluationContext.measuredTimes.stream().reduce(0L, (a, b) -> a + b);
        System.out.println("Overall results:");
        System.out.printf("Measured %s; estimated %s.\n", overallMeasuredTime, overallTimeEstimate);
    }

    private static class EvaluationContext {

        private final List<TimeEstimate> timeEstimates = new LinkedList<>();

        private final List<Long> measuredTimes = new LinkedList<>();

    }
}
