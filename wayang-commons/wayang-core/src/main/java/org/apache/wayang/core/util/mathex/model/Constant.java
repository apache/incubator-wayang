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

package org.apache.wayang.core.util.mathex.model;

import org.apache.wayang.core.util.mathex.Context;
import org.apache.wayang.core.util.mathex.Expression;

/**
 * A constant {@link Expression}.
 */
public class Constant implements Expression {

    final double value;

    public Constant(double value) {
        this.value = value;
    }

    public double getValue() {
        return this.value;
    }

    @Override
    public double evaluate(Context context) {
        return this.value;
    }

    @Override
    public Expression specify(Context context) {
        return this;
    }

    @Override
    public String toString() {
        return Double.toString(this.value);
    }
}
