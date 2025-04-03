/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.wayang.java.operators;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import org.apache.wayang.basic.operators.AzureBlobStorageSource;
import org.apache.wayang.core.api.exception.WayangException;
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

/**
 * This is execution operator that implements the {@link AzureBlobStorageSource}.
 */

public class JavaAzureBlobStorageSource extends AzureBlobStorageSource implements JavaExecutionOperator {
    private static final Logger logger = LoggerFactory.getLogger(JavaAzureBlobStorageSource.class);

    public JavaAzureBlobStorageSource(String storageContainer, String blobName, String filePathToCredentialsFile) {
        super(storageContainer, blobName, filePathToCredentialsFile);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */

    public JavaAzureBlobStorageSource(AzureBlobStorageSource that) {
        super(that);
    }


    @Override
    public Collection<String> getLoadProfileEstimatorConfigurationKeys() {
        return Arrays.asList("wayang.java.azureblobstoragesource.load.prepare", "wayang.java.azureblobstoragesource.load.main");
    }

    @Override
    public JavaAmazonS3Source copy() {
        return new JavaAmazonS3Source(this.getStorageContainer(), this.getBlobName(), this.getFilePathToCredentialsFile());
    }
   
    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        throw new UnsupportedOperationException(String.format("%s does not have input channels.", this));
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
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


     ExecutionLineageNode prepareLineageNode = new ExecutionLineageNode(operatorContext);
        prepareLineageNode.add(LoadProfileEstimators.createFromSpecification(

                "wayang.java.azureblobstoragesource.load.prepare", javaExecutor.getConfiguration()
        ));
        ExecutionLineageNode mainLineageNode = new ExecutionLineageNode(operatorContext);
        mainLineageNode.add(LoadProfileEstimators.createFromSpecification(
                "wayang.java.azureblobstoragesource.load.main", javaExecutor.getConfiguration()
        ));


        outputs[0].getLineage().addPredecessor(mainLineageNode);

        return prepareLineageNode.collectAndMark();
    }
    
}
