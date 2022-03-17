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

package org.apache.wayang.iejoin.operators;

import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.data.Tuple5;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.plan.wayangplan.BinaryToUnaryOperator;
import org.apache.wayang.core.types.DataSetType;

/**
 * This operator applies inequality join on elements of input datasets.
 */
public class IEJoinOperator<Type0, Type1, Input>
        extends BinaryToUnaryOperator<Input, Input, Tuple2<Input, Input>> {

    //protected final int get0Pivot;
    protected final TransformationDescriptor<Input, Type0> get0Pivot;
    //protected final int get1Pivot;
    protected final TransformationDescriptor<Input, Type0> get1Pivot;
    protected final IEJoinMasterOperator.JoinCondition cond0;
    //protected final int get0Ref;
    protected final TransformationDescriptor<Input, Type1> get0Ref;
    //protected final int get1Ref;
    protected final TransformationDescriptor<Input, Type1> get1Ref;
    protected final IEJoinMasterOperator.JoinCondition cond1;
    protected boolean list1ASC;
    protected boolean list1ASCSec;
    protected boolean list2ASC;
    protected boolean list2ASCSec;
    protected boolean equalReverse;

    public IEJoinOperator(Class<Input> inputType0Class, Class<Input> inputType1Class,
                          TransformationDescriptor<Input, Type0> get0Pivot, TransformationDescriptor<Input, Type0> get1Pivot, IEJoinMasterOperator.JoinCondition cond0,
                          TransformationDescriptor<Input, Type1> get0Ref, TransformationDescriptor<Input, Type1> get1Ref, IEJoinMasterOperator.JoinCondition cond1) {
        super(DataSetType.createDefault(inputType0Class),
                DataSetType.createDefault(inputType1Class),
                DataSetType.createDefaultUnchecked(Tuple2.class),
                false);
        this.get0Pivot = get0Pivot;
        this.get1Pivot = get1Pivot;
        this.cond0 = cond0;
        this.get0Ref = get0Ref;
        this.get1Ref = get1Ref;
        this.cond1 = cond1;
        assignSortOrders();
    }

    public IEJoinOperator(DataSetType<Input> inputType0, DataSetType<Input> inputType1,
                          TransformationDescriptor<Input, Type0> get0Pivot, TransformationDescriptor<Input, Type0> get1Pivot, IEJoinMasterOperator.JoinCondition cond0,
                          TransformationDescriptor<Input, Type1> get0Ref, TransformationDescriptor<Input, Type1> get1Ref, IEJoinMasterOperator.JoinCondition cond1) {
        super(inputType0, inputType1, DataSetType.createDefaultUnchecked(Tuple2.class), false);
        this.get0Pivot = get0Pivot;
        this.get1Pivot = get1Pivot;
        this.cond0 = cond0;
        this.get0Ref = get0Ref;
        this.get1Ref = get1Ref;
        this.cond1 = cond1;
        assignSortOrders();
    }

    public void assignSortOrders() {
        Tuple5<Boolean, Boolean, Boolean, Boolean, Boolean> sortOrders = IEJoinMasterOperator.getSortOrders(this.cond0, this.cond1);
        list1ASC = sortOrders.getField0();
        list1ASCSec = sortOrders.getField1();
        list2ASC = sortOrders.getField2();
        list2ASCSec = sortOrders.getField3();
        equalReverse = sortOrders.getField4();
    }

    public TransformationDescriptor<Input, Type0> getGet0Pivot() {
        return this.get0Pivot;
    }

    public TransformationDescriptor<Input, Type1> getGet0Ref() {
        return this.get0Ref;
    }

    public TransformationDescriptor<Input, Type0> getGet1Pivot() {
        return this.get1Pivot;
    }

    public TransformationDescriptor<Input, Type1> getGet1Ref() {
        return this.get1Ref;
    }

    public IEJoinMasterOperator.JoinCondition getCond0() {
        return this.cond0;
    }

    public IEJoinMasterOperator.JoinCondition getCond1() {
        return this.cond1;
    }
}
