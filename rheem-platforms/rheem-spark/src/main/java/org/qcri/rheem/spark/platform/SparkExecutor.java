package org.qcri.rheem.spark.platform;

import org.qcri.rheem.core.plan.ExecutionOperator;
import org.qcri.rheem.core.platform.Executor;

import org.qcri.rheem.spark.compiler.FunctionCompiler;


public class SparkExecutor implements Executor{

    public static final Executor.Factory FACTORY = () -> new SparkExecutor();

    public FunctionCompiler compiler = new FunctionCompiler();

    @Override
    public void evaluate(ExecutionOperator executionOperator) {
    }
}
