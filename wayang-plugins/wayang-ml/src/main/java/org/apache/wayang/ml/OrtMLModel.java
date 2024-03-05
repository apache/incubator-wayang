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

package org.apache.wayang.ml;

import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtException;
import ai.onnxruntime.OrtSession;
import ai.onnxruntime.OrtSession.Result;
import org.apache.wayang.core.api.Configuration;
import org.apache.commons.lang3.ArrayUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.HashSet;
import java.util.function.BiFunction;

public class OrtMLModel {

    private static OrtMLModel INSTANCE;

    private OrtSession session;
    private OrtEnvironment env;

    private final Map<String, OnnxTensor> inputMap = new HashMap<>();
    private final Set<String> requestedOutputs = new HashSet<>();

    public static OrtMLModel getInstance(Configuration configuration) throws OrtException {
        if (INSTANCE == null) {
            INSTANCE = new OrtMLModel(configuration);
        }

        return INSTANCE;
    }

    private OrtMLModel(Configuration configuration) throws OrtException {
        this.loadModel(configuration.getStringProperty("wayang.ml.model.file"));
    }

    private void loadModel(String filePath) throws OrtException {
        if (this.env == null) {
            this.env = OrtEnvironment.getEnvironment();
        }

        if (this.session == null) {
            this.session = env.createSession(filePath, new OrtSession.SessionOptions());
        }
    }

    /**
     * Close the session after running, {@link #closeSession()}
     * @param encodedVector
     * @return NaN on error, and a predicted cost on any other value.
     * @throws OrtException
     */
    public double runModel(long[] encodedVector) throws OrtException {
        double costPrediction;

        OnnxTensor tensor = OnnxTensor.createTensor(env, encodedVector);
        this.inputMap.put("input", tensor);
        this.requestedOutputs.add("output");

        BiFunction<Result, String, Float> unwrapFunc = (r, s) -> {
            try {
                return ((float[]) r.get(s).get().getValue())[0];
            } catch (OrtException e) {
                return Float.NaN;
            }
        };

        try (Result r = session.run(inputMap, requestedOutputs)) {
            costPrediction = unwrapFunc.apply(r, "output");
        } finally {
            inputMap.clear();
            requestedOutputs.clear();
        }


        return costPrediction;
    }

    /**
     * Closes the OrtModel resource, relinquishing any underlying resources.
     * @throws OrtException
     */
    public void closeSession() throws OrtException {
        this.session.close();
        this.env.close();
    }
}
