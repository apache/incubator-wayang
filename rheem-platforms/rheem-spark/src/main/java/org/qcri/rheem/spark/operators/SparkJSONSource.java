package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.qcri.rheem.basic.data.RecordDinamic;
import org.qcri.rheem.basic.operators.JSONSource;
import org.qcri.rheem.basic.util.JSONParser;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.spark.channels.RddChannel;
import org.qcri.rheem.spark.execution.SparkExecutor;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Provides a {@link Collection} to a Spark job.
 */
public class SparkJSONSource extends JSONSource implements SparkExecutionOperator {

    public SparkJSONSource(String inputUrl) {
        super(inputUrl);
    }

    public SparkJSONSource(String inputUrl, String encoding) {
        super(inputUrl, encoding);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public SparkJSONSource(JSONSource that) {
        super(that);
    }

    public SparkJSONSource(String inputUrl, String[] words){
        super(inputUrl, words);
    }

    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(ChannelInstance[] inputs, ChannelInstance[] outputs, SparkExecutor sparkExecutor, OptimizationContext.OperatorContext operatorContext) {
        //create the struct for reading
        SparkTextFileSource textOperator = new SparkTextFileSource(this.getFile());
        Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> element = textOperator.evaluate(inputs, outputs, sparkExecutor, operatorContext);

        //get the instance of the reader of the file
        RddChannel.Instance instance = ((RddChannel.Instance) outputs[0]);

        //get the rdd and words for parser
        JavaRDD<String> rdd = instance.provideRdd();
        String[] words = this.getWords_tokens();

        //generate the expression for mapping of the JSON
        final TransformationDescriptor<String, RecordDinamic> lambda = new TransformationDescriptor<String, RecordDinamic>(
            line -> {
                return JSONParser.execute(line, words);
            },
            String.class,
                RecordDinamic.class
        );

        //compile the function for runing in spark
        final Function<String, RecordDinamic> mapFunctions =
            sparkExecutor.getCompiler().compile(
                lambda,
                this,
                operatorContext,
                inputs
            );
        JavaRDD<RecordDinamic> newReader = rdd.map(mapFunctions);
        this.name(newReader);

        //set the output with the new reader.
        ((RddChannel.Instance) outputs[0]).accept(newReader, sparkExecutor);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    protected ExecutionOperator createCopy() {
        return new SparkJSONSource(this);
    }

    public Collection<String> getLoadProfileEstimatorConfigurationKeys() {
        return Arrays.asList(new String[]{"rheem.spark.textfilesource.load.prepare", "rheem.spark.textfilesource.load.main"});
    }

    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        throw new UnsupportedOperationException(String.format("%s does not have input channels.", new Object[]{this}));
    }

    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(RddChannel.UNCACHED_DESCRIPTOR);
    }

    public boolean containsAction() {
        return false;
    }




}
