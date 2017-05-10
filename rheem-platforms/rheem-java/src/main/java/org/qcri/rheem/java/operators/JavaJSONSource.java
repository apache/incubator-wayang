package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.data.RecordDinamic;
import org.qcri.rheem.basic.operators.JSONSource;
import org.qcri.rheem.basic.util.JSONParser;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.java.execution.JavaExecutor;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

/**
 * This is execution operator implements the {@link JSONSource}.
 */
public class JavaJSONSource extends JSONSource implements JavaExecutionOperator {

    JavaTextFileSource file_source = null;

    public JavaJSONSource(String inputUrl) {
        super(inputUrl);
        this.file_source = new JavaTextFileSource(inputUrl);
    }

    public JavaJSONSource(String inputUrl, String encoding) {
        super(inputUrl, encoding);
        this.file_source = new JavaTextFileSource(inputUrl);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public JavaJSONSource(JSONSource that) {
        super(that);
        this.file_source = new JavaTextFileSource(that.getFile());
    }

    public JavaJSONSource(String inputUrl, String[] words){
        super(inputUrl, words);
        this.file_source = new JavaTextFileSource(inputUrl);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(ChannelInstance[] inputs, ChannelInstance[] outputs, JavaExecutor javaExecutor, OptimizationContext.OperatorContext operatorContext) {
        //load the configuration of reading
        this.file_source.evaluate(inputs, outputs, javaExecutor, operatorContext);
        //get the original stream, for aplicate the new filter
        Stream<String> stream = ((StreamChannel.Instance) outputs[0]).provideStream();
        //add the new filter over the stream
        Stream<RecordDinamic> record = stream.map(line -> {return JSONParser.execute(line, this.getWords_tokens());});
        //the new stream is asigned to the output
        ((StreamChannel.Instance) outputs[0]).accept(record);
        //load the parameters for future execution
        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    public Collection<String> getLoadProfileEstimatorConfigurationKeys() {
        return Arrays.asList("rheem.java.textfilesource.load.prepare", "rheem.java.textfilesource.load.main");
    }
    @Override
    public JavaJSONSource copy() {
        return new JavaJSONSource(this);
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        throw new UnsupportedOperationException(String.format("%s does not have input channels.", new Object[]{this}));
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || index == 0 && this.getNumOutputs() == 0;
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }


}
