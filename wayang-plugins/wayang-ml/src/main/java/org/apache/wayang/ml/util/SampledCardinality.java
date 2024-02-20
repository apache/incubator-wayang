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

package org.apache.wayang.ml.util;

import org.apache.wayang.core.util.JsonSerializer;
import org.apache.wayang.core.util.json.WayangJsonArray;
import org.apache.wayang.core.util.json.WayangJsonObj;

public class SampledCardinality {

    private WayangJsonArray inputs;

    private WayangJsonObj operator;

    private WayangJsonObj output;

    public SampledCardinality(
        WayangJsonArray inputs,
        WayangJsonObj operator,
        WayangJsonObj output
    ) {
        this.inputs = inputs;
        this.operator = operator;
        this.output = output;
    }

    public WayangJsonArray getInputs() {
        return this.inputs;
    }

    public WayangJsonObj getOperator() {
        return this.operator;
    }
    public WayangJsonObj getOutput() {
        return this.output;
    }

    public static class Serializer implements JsonSerializer<SampledCardinality> {

        @Override
        public WayangJsonObj serialize(SampledCardinality sample) {
            return new WayangJsonObj();
        }

        @Override
        public SampledCardinality deserialize(WayangJsonObj json, Class<? extends SampledCardinality> cls) {
            final WayangJsonArray inputs = json.getJSONArray("inputs");
            final WayangJsonObj operator = json.getJSONObject("operator");
            final WayangJsonObj output = json.getJSONObject("output");

            return new SampledCardinality(
                inputs, operator, output
            );
        }
    }
}
