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

package org.apache.wayang.core.util.mathex;

import org.apache.wayang.core.util.mathex.exceptions.EvaluationException;

import java.util.function.ToDoubleFunction;

/**
 * Provides contextual variables and functions in order to evaluate an {@link Expression}.
 */
public interface Context {

    Context baseContext = DefaultContext.createBaseContext();


    /**
     * Provide the value for a variable.
     *
     * @param variableName the name of the variable
     * @return the variable value
     * @throws EvaluationException if the variable request could not be served
     */
    double getVariable(String variableName) throws EvaluationException;

    /**
     * Provide the function.
     *
     * @param functionName the name of the function
     * @return the function
     * @throws EvaluationException if the variable request could not be served
     */
    ToDoubleFunction<double[]> getFunction(String functionName) throws EvaluationException;

}
