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

public class Adam extends Optimizer {
    private final float betaOne;
    private final float betaTwo;
    private final float epsilon;

    public Adam(float learningRate) {
        super(learningRate);
        this.betaOne = 0.9f;
        this.betaTwo = 0.999f;
        this.epsilon = 1e-8f;
    }

    public Adam(float learningRate, String name) {
        super(learningRate, name);
        this.betaOne = 0.9f;
        this.betaTwo = 0.999f;
        this.epsilon = 1e-8f;
    }

    public Adam(float learningRate, float betaOne, float betaTwo, float epsilon, String name) {
        super(learningRate, name);
        this.betaOne = betaOne;
        this.betaTwo = betaTwo;
        this.epsilon = epsilon;
    }

    public float getBetaOne() {
        return betaOne;
    }

    public float getBetaTwo() {
        return betaTwo;
    }

    public float getEpsilon() {
        return epsilon;
    }
}
