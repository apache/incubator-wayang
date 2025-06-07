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

package org.apache.wayang.basic.model.op;

import org.apache.wayang.basic.model.DLModel;
import org.apache.wayang.basic.model.op.nn.CrossEntropyLoss;
import org.apache.wayang.basic.model.op.nn.Linear;
import org.apache.wayang.basic.model.op.nn.ReLU;
import org.junit.jupiter.api.Test;

class OpTest {

    @Test
    void testBuild() {
        // model
        Input features = new Input(Input.Type.FEATURES);
        Input labels = new Input(Input.Type.LABEL);

        DLModel model = new DLModel.Builder()
                .layer(features)
                .layer(new Linear(4, 4, true, "l1"))
                .layer(new ReLU("r1"))
                .layer(new Linear(4, 3, true, "l2"))
                .build();

        // loss function
        CrossEntropyLoss loss = new CrossEntropyLoss(3, "loss");
        loss.with(
                model.getOut(),
                labels
        );

        // accuracy calculation
        Mean acc = new Mean(0);
        acc.with(
                new Cast(Op.DType.FLOAT32).with(
                        new Eq().with(
                                new ArgMax(1).with(
                                        model.getOut()
                                ),
                                labels
                        )
                )
        );

        System.out.println();
    }
}