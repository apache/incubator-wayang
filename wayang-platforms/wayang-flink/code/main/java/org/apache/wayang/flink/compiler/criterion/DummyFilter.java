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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

/**
 * Create a {@Link FilterFunction} that remove the elements null
 */
public class DummyFilter<InputType> implements FilterFunction<InputType>, ResultTypeQueryable<InputType> {


    public final Class<InputType>  inputTypeClass;
    private final TypeInformation<InputType> typeInformationInput;

    public DummyFilter(Class<InputType>  inputTypeClass){
        this.inputTypeClass  = inputTypeClass;
        this.typeInformationInput   = TypeInformation.of(this.inputTypeClass);
    }


    @Override
    public boolean filter(InputType inputType) throws Exception {
        return inputType != null;
    }

    @Override
    public TypeInformation<InputType> getProducedType() {
        return this.typeInformationInput;
    }

}
