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

package org.apache.wayang.core.profiling;


import org.apache.wayang.commons.util.profiledb.ProfileDB;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.OperatorBase;
import org.apache.wayang.core.plan.wayangplan.PlanMetrics;

/**
 * Utilities to work with {@link ProfileDB}s.
 */
public class ProfileDBs {

    /**
     * Create and customize a {@link ProfileDB}.
     *
     * @return the {@link ProfileDB}
     */
    public static ProfileDB createProfileDB() {
        final ProfileDB profileDB = new ProfileDB(null);
        customize(profileDB);
        return profileDB;
    }

    /**
     * Customize a {@link ProfileDB} for use with Wayang.
     *
     * @param profileDB the {@link ProfileDB}
     */
    public static void customize(ProfileDB profileDB) {
        profileDB
                .withGsonPreparation(
                        gsonBuilder -> gsonBuilder.registerTypeAdapter(Operator.class, new OperatorBase.GsonSerializer())
                )
                .registerMeasurementClass(CostMeasurement.class)
                .registerMeasurementClass(PlanMetrics.class);
    }

}
