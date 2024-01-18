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

package org.apache.wayang.agoraeo.operators.basic;

import org.apache.wayang.core.plan.wayangplan.UnaryToUnaryOperator;
import org.apache.wayang.core.types.BasicDataUnitType;
import org.apache.wayang.core.types.DataSetType;

public class Sen2CorWrapper
    extends UnaryToUnaryOperator<String, String>
{
    public String sen2cor;
    public String l2a_location;

    public Sen2CorWrapper(String sen2cor, String l2a_location){
        super(
                DataSetType.createDefault(BasicDataUnitType.createBasic(String.class)),
                DataSetType.createDefault(BasicDataUnitType.createBasic(String.class)),
                false
        );
        this.sen2cor = sen2cor;
        this.l2a_location = l2a_location;
    }

    public Sen2CorWrapper(UnaryToUnaryOperator<String, String> that) {
        super(that);
    }


}
