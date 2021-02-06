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

package org.apache.wayang.jdbc.compiler;

import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.function.PredicateDescriptor;

/**
 * Compiles {@link FunctionDescriptor}s to SQL clauses.
 */
public class FunctionCompiler {

    /**
     * Compile a predicate to a SQL {@code WHERE} clause.
     *
     * @param descriptor describes the predicate
     * @return a compiled SQL {@code WHERE} clause
     */
    public String compile(PredicateDescriptor descriptor) {
        final String sqlImplementation = descriptor.getSqlImplementation();
        assert sqlImplementation != null;
        return sqlImplementation;
    }

}
