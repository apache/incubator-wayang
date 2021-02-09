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

package org.apache.wayang.core.mapping.test;

import org.apache.wayang.core.mapping.OperatorMatch;
import org.apache.wayang.core.mapping.ReplacementSubplanFactory;
import org.apache.wayang.core.mapping.SubplanMatch;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.test.TestSink;
import org.apache.wayang.core.plan.wayangplan.test.TestSink2;

/**
 * This factory replaces a {@link TestSink} by a
 * {@link TestSink2}.
 */
public class TestSinkToTestSink2Factory extends ReplacementSubplanFactory {

    @Override
    protected Operator translate(SubplanMatch subplanMatch, int epoch) {
        // Retrieve the matched TestSink.
        final OperatorMatch sinkMatch = subplanMatch.getOperatorMatches().get("sink");
        final TestSink testSink = (TestSink) sinkMatch.getOperator();

        // Translate the TestSink to a TestSink2.
        return new TestSink2<>(testSink.getInput().getType()).at(epoch);
    }

}
