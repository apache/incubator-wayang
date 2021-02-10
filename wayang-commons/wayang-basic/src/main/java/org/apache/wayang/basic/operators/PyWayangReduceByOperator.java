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

package org.apache.wayang.basic.operators;

import com.google.protobuf.ByteString;
import org.apache.wayang.core.plan.wayangplan.UnaryToUnaryOperator;
import org.apache.wayang.core.types.DataSetType;

import java.util.Map;

public class PyWayangReduceByOperator<Type, Key> extends UnaryToUnaryOperator<Type, Type> {

    protected final Map<String, String> parameters;
    protected final ByteString reduceDescriptor;

    public PyWayangReduceByOperator(Map<String, String> parameters,
                                       ByteString reduceDescriptor,
                                       DataSetType<Type> inputType, DataSetType<Type> outputType, boolean isSupportingBroadcastInputs) {
        super(inputType, outputType, isSupportingBroadcastInputs);
        this.parameters = parameters;
        this.reduceDescriptor = reduceDescriptor;
    }

    public PyWayangReduceByOperator(Map<String, String> parameters,
                                    ByteString reduceDescriptor,
                                    Class<Type> inputType, Class<Type> outputType, boolean isSupportingBroadcastInputs) {
        super(DataSetType.createDefault(inputType), DataSetType.createDefault(outputType), isSupportingBroadcastInputs);
        this.parameters = parameters;
        this.reduceDescriptor = reduceDescriptor;
    }

    public PyWayangReduceByOperator(PyWayangReduceByOperator<Type, Type> that) {
        super(that);
        this.parameters = that.getParameters();
        this.reduceDescriptor = that.getReduceDescriptor();
    }

    public Map<String, String>  getParameters() {
        return this.parameters;
    }

    public ByteString getReduceDescriptor() {
        return this.reduceDescriptor;
    }

}