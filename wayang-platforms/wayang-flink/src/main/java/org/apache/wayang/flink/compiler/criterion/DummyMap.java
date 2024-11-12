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

package org.apache.wayang.flink.compiler.criterion;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

/**
 * Class create a {@Link MapFunction} that genereta only null as convertion
 */
public class DummyMap<InputType, OutputType> implements MapFunction<InputType, OutputType>, ResultTypeQueryable<OutputType> {

    public final Class<InputType>  inputTypeClass;
    public final Class<OutputType> outputTypeClass;
    private final TypeInformation<InputType>  typeInformationInput;
    private final TypeInformation<OutputType> typeInformationOutput;

    public DummyMap(Class<InputType>  inputTypeClass, Class<OutputType> outputTypeClass){
        this.inputTypeClass  = inputTypeClass;
        this.outputTypeClass = outputTypeClass;
        this.typeInformationInput   = TypeInformation.of(this.inputTypeClass);
        this.typeInformationOutput  = TypeInformation.of(this.outputTypeClass);
    }


    @Override
    public OutputType map(InputType inputType) throws Exception {
        return null;
    }

    @Override
    public TypeInformation<OutputType> getProducedType() {
        return this.typeInformationOutput;
    }


}
