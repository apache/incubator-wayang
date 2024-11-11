package org.apache.wayang.java.operators;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import org.apache.wayang.basic.operators.AmazonS3Source;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.OptimizationContext.OperatorContext;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimators;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JavaAmazonS3Source extends AmazonS3Source implements JavaExecutionOperator {
    private static final Logger logger = LoggerFactory.getLogger(JavaTextFileSource.class);

    public JavaAmazonS3Source(String bucket, String blobName, String filePathToCredentialsFile) throws IOException {
        super(bucket, blobName, filePathToCredentialsFile);
    }

    public JavaAmazonS3Source(AmazonS3Source that) {
        super(that);
    }





    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getSupportedInputChannels'");
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getSupportedOutputChannels'");
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(ChannelInstance[] inputs,
            ChannelInstance[] outputs, JavaExecutor javaExecutor, OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        try {
            
            BufferedReader buffereadReder = new BufferedReader(new InputStreamReader(super.getInputStream()));
            Stream<String> lines = buffereadReder.lines(); 
            ((StreamChannel.Instance) outputs[0]).accept(lines);
        }
        catch (Exception e) {
            throw new WayangException("Failed to read file from Amazon storage with error", e);
        }


    //TODO what to write here
     ExecutionLineageNode prepareLineageNode = new ExecutionLineageNode(operatorContext);
        prepareLineageNode.add(LoadProfileEstimators.createFromSpecification(
                //TODO what to write here

                "wayang.java.amazons3source.load.prepare", javaExecutor.getConfiguration()
        ));
        ExecutionLineageNode mainLineageNode = new ExecutionLineageNode(operatorContext);
        mainLineageNode.add(LoadProfileEstimators.createFromSpecification(
                //TODO what to write here

                "wayang.java.amazons3source.load.main", javaExecutor.getConfiguration()
        ));


        outputs[0].getLineage().addPredecessor(mainLineageNode);

        return prepareLineageNode.collectAndMark();
    }
    
    
}
