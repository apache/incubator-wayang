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

package org.apache.wayang.iejoin.operators.spark_helpers;

import org.apache.spark.api.java.function.Function;
import org.apache.wayang.iejoin.data.Data;
import scala.Tuple2;

/**
 * Created by khayyzy on 5/28/16.
 */
public class extractData<TypeXPivot extends Comparable<TypeXPivot>,
        TypeXRef extends Comparable<TypeXRef>, Input> implements Function<Tuple2<Long, Input>, Data<TypeXPivot, TypeXRef>> {

    /**
     *
     */
    private static final long serialVersionUID = 3834945091845558509L;
    org.apache.spark.api.java.function.Function<Input, TypeXPivot> getXPivot;
    org.apache.spark.api.java.function.Function<Input, TypeXRef> getXRef;

    public extractData(org.apache.spark.api.java.function.Function<Input, TypeXPivot> getXPivot, org.apache.spark.api.java.function.Function<Input, TypeXRef> getXRef) {
        this.getXPivot = getXPivot;
        this.getXRef = getXRef;
    }

    public Data call(Tuple2<Long, Input> in) throws Exception {
        return new Data<TypeXPivot, TypeXRef>(in._1(),
                //(TypeXPivot) in._2().getField(getXPivot),
                //(TypeXRef) in._2().getField(getXRef));
                this.getXPivot.call(in._2()), this.getXRef.call(in._2()));
    }
}
