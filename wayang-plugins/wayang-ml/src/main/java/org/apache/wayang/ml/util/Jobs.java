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

import org.apache.wayang.ml.training.GeneratableJob;
import org.reflections.*;

import java.util.Set;
import java.util.Comparator;

public class Jobs {
    public static Set<Class<? extends GeneratableJob>> getJobs() {
        Reflections reflections = new Reflections("org.apache.wayang.ml.generatables");
        return reflections.getSubTypesOf(GeneratableJob.class);
    }

    public static Class<? extends GeneratableJob> getJob(int index) {

        Set<Class<? extends GeneratableJob>> jobs = new Reflections("org.apache.wayang.ml.generatables").getSubTypesOf(GeneratableJob.class);

        return jobs.stream()
            .sorted(Comparator.comparing(c -> c.getName()))
            .skip(index)
            .findFirst()
            .get();
    }
}

