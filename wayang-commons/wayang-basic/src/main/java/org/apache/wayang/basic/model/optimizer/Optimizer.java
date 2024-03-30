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

package org.apache.wayang.basic.model.optimizer;

import java.util.concurrent.atomic.AtomicInteger;

public abstract class Optimizer {
    protected final AtomicInteger CNT = new AtomicInteger(0);
    protected final float learningRate;

    protected final String name;

    protected Optimizer(float learningRate, String name) {
        this.learningRate = learningRate;
        this.name = name;
    }

    protected Optimizer(float learningRate) {
        this.learningRate = learningRate;
        this.name = this.getClass().getSimpleName() + CNT.getAndIncrement();
    }

    public String getName() {
        return name;
    }

    public float getLearningRate() {
        return learningRate;
    }
}
