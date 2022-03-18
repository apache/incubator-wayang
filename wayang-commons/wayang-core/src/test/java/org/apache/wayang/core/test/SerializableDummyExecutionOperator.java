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

package org.apache.wayang.core.test;

import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.util.JsonSerializable;
import org.apache.wayang.core.util.json.WayangJsonObj;

/**
 * Dummy {@link ExecutionOperator} for test purposes.
 */
public class SerializableDummyExecutionOperator extends DummyExecutionOperator implements JsonSerializable {

    public SerializableDummyExecutionOperator(int someProperty) {
        super(1, 1, false);
        this.setSomeProperty(someProperty);
    }

    @Override
    public WayangJsonObj toJson() {
        return new WayangJsonObj().put("someProperty", this.getSomeProperty());
    }

    @SuppressWarnings("unused")
    public static SerializableDummyExecutionOperator fromJson(WayangJsonObj wayangJsonObj) {
        return new SerializableDummyExecutionOperator(wayangJsonObj.getInt("someProperty"));
    }
}
