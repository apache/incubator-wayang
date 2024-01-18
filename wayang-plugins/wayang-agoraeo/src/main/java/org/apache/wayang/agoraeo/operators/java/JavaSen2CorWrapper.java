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

package org.apache.wayang.agoraeo.operators.java;

import org.apache.wayang.agoraeo.operators.basic.Sen2CorWrapper;
import org.apache.wayang.core.optimizer.OptimizationContext.OperatorContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.java.channels.CollectionChannel;
import org.apache.wayang.java.channels.JavaChannelInstance;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;
import org.apache.wayang.java.operators.JavaExecutionOperator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.function.Function;
import java.util.stream.StreamSupport;

//TODO add the documentation and add the Profile Estimator
public class JavaSen2CorWrapper
    extends Sen2CorWrapper
    implements JavaExecutionOperator {


  public JavaSen2CorWrapper(String sen2cor, String l2a_location) {
    super(sen2cor, l2a_location);
  }

  @Override
  public List<ChannelDescriptor> getSupportedInputChannels(int index) {
    assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
    if (this.getInput(index).isBroadcast()) return Collections.singletonList(CollectionChannel.DESCRIPTOR);
    return Arrays.asList(CollectionChannel.DESCRIPTOR, StreamChannel.DESCRIPTOR);
  }

  @Override
  public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
    assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
    return Collections.singletonList(StreamChannel.DESCRIPTOR);
  }

  @Override
  public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(ChannelInstance[] inputs, ChannelInstance[] outputs, JavaExecutor javaExecutor, OperatorContext operatorContext) {

      assert inputs.length == this.getNumInputs();
      assert outputs.length == this.getNumOutputs();

      final Function<String, Iterable<String>> flatmapFunction =
              t -> {
                  System.out.println("sen2cor");
                  System.out.println(sen2cor);
                  System.out.println("l2a_location");
                  System.out.println(l2a_location);
                  try {
                      String command = sen2cor + " " +
                              t + " " +
                              " --output_dir " + l2a_location;
                      Process process = Runtime.getRuntime().exec(command);
                      Iterator<String> input = new BufferedReader(
                              new InputStreamReader(
                                      process.getInputStream()
                              )
                      ).lines().iterator();
                      return () -> input;
                  } catch (IOException e) {
                      throw new RuntimeException(e);
                  }
              };
//            javaExecutor.getCompiler().compile(this.functionDescriptor);

      JavaExecutor.openFunction(this, flatmapFunction, inputs, operatorContext);

      ((StreamChannel.Instance) outputs[0]).accept(
              ((JavaChannelInstance) inputs[0]).<String>provideStream().flatMap(dataQuantum ->
                      StreamSupport.stream(
                              Spliterators.spliteratorUnknownSize(
                                      flatmapFunction.apply(dataQuantum).iterator(),
                                      Spliterator.ORDERED),
                              false
                      )
              )
      );

      return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
  }

    public JavaSen2CorWrapper(Sen2CorWrapper that) {
      super(that);
      this.sen2cor = that.sen2cor;
      this.l2a_location = that.l2a_location;
    }

    protected ExecutionOperator createCopy() {
        return new JavaSen2CorWrapper(this.sen2cor, this.l2a_location);
    }
}
