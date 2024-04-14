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

package org.apache.wayang.ml.util;

import org.apache.wayang.core.plan.wayangplan.OperatorBase;

import org.reflections.*;

import java.util.stream.Collectors;
import java.util.Set;
import java.util.Comparator;

public class Operators {
    public static Set<Class<? extends OperatorBase>> getOperators() {
        Reflections reflections = new Reflections("org.apache.wayang.basic.operators");
        Set<Class<? extends OperatorBase>> basics = reflections.getSubTypesOf(OperatorBase.class);

        Reflections coreReflections = new Reflections("org.apache.wayang.core.plan.wayangplan");
        Set<Class<? extends OperatorBase>> core = reflections.getSubTypesOf(OperatorBase.class);

        basics.addAll(core);
        return basics;
    }

    public static Set<Class<? extends OperatorBase>> getPlatformOperators(String namespace) {
        Reflections reflections = new Reflections(namespace + ".operators");
        return reflections.getSubTypesOf(OperatorBase.class)
            .stream()
            .filter(operator -> operator.getName().contains(namespace))
            .distinct()
            .sorted(Comparator.comparing(c -> c.getName()))
            .collect(Collectors.toSet());
    }
}
