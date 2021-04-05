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

package org.apache.wayang.iejoin.operators.java_helpers;


import org.apache.wayang.iejoin.data.Data;

import java.util.function.Function;

/**
 * Created by khayyzy on 5/28/16.
 */
public class extractData<TypeXPivot extends Comparable<TypeXPivot>,
        TypeXRef extends Comparable<TypeXRef>, Input> {

    /**
     *
     */
    private static final long serialVersionUID = 3834945091845558509L;
    Function<Input, TypeXPivot> getXPivot;
    Function<Input, TypeXRef> getXRef;

    public extractData(Function<Input, TypeXPivot> getXPivot, Function<Input, TypeXRef> getXRef) {
        this.getXPivot = getXPivot;
        this.getXRef = getXRef;
    }

    public Data call(Input in) {
        return new Data<TypeXPivot, TypeXRef>(-1,
                //  (TypeXPivot) in.getField(getXPivot),
                // (TypeXRef) in.getField(getXRef));
                getXPivot.apply(in),
                getXRef.apply(in));
    }
}
