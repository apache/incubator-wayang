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

package org.apache.wayang.tensorflow.platform;

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.optimizer.costs.LoadProfileToTimeConverter;
import org.apache.wayang.core.optimizer.costs.LoadToTimeConverter;
import org.apache.wayang.core.optimizer.costs.TimeToCostConverter;
import org.apache.wayang.core.platform.Executor;
import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.tensorflow.execution.TensorflowContextReference;
import org.apache.wayang.tensorflow.execution.TensorflowExecutor;

/**
 * {@link Platform} for Tensorflow
 */
public class TensorflowPlatform extends Platform {

    private static final String PLATFORM_NAME = "Tensorflow";

    private static final String CONFIG_NAME = "tensorflow";

    private static final String DEFAULT_CONFIG_FILE = "wayang-tensorflow-defaults.properties";

    private static TensorflowPlatform instance = null;

    private TensorflowContextReference tensorflowContextReference;

    public static TensorflowPlatform getInstance() {
        if (instance == null) {
            instance = new TensorflowPlatform();
        }
        return instance;
    }

    private TensorflowPlatform() {
        super(PLATFORM_NAME, CONFIG_NAME);
    }

    @Override
    protected void configureDefaults(Configuration configuration) {
        configuration.load(ReflectionUtils.loadResource(DEFAULT_CONFIG_FILE));
    }

    @Override
    public Executor.Factory getExecutorFactory() {
        return job -> new TensorflowExecutor(this, job);
    }

    @Override
    public LoadProfileToTimeConverter createLoadProfileToTimeConverter(Configuration configuration) {
        int cpuMhz = (int) configuration.getLongProperty("wayang.tensorflow.cpu.mhz");
        int numCores = (int) configuration.getLongProperty("wayang.tensorflow.cores");
        double hdfsMsPerMb = configuration.getDoubleProperty("wayang.tensorflow.hdfs.ms-per-mb");
        return LoadProfileToTimeConverter.createDefault(
                LoadToTimeConverter.createLinearCoverter(1 / (numCores * cpuMhz * 1000d)),
                LoadToTimeConverter.createLinearCoverter(hdfsMsPerMb / 1000000d),
                LoadToTimeConverter.createLinearCoverter(0),
                (cpuEstimate, diskEstimate, networkEstimate) -> cpuEstimate.plus(diskEstimate).plus(networkEstimate)
        );
    }

    @Override
    public TimeToCostConverter createTimeToCostConverter(Configuration configuration) {
        return new TimeToCostConverter(
                configuration.getDoubleProperty("wayang.tensorflow.costs.fix"),
                configuration.getDoubleProperty("wayang.tensorflow.costs.per-ms")
        );
    }

    public TensorflowContextReference getTensorflowContext(Job job) {
        if (this.tensorflowContextReference == null || this.tensorflowContextReference.isDisposed()) {
            this.tensorflowContextReference = new TensorflowContextReference(job.getCrossPlatformExecutor());
        }
        return this.tensorflowContextReference;
    }
}
