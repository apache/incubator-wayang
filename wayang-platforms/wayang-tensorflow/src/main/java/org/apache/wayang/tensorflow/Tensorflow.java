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

package org.apache.wayang.tensorflow;

import org.apache.wayang.tensorflow.platform.TensorflowPlatform;
import org.apache.wayang.tensorflow.plugin.TensorflowConversionPlugin;
import org.apache.wayang.tensorflow.plugin.TensorflowPlugin;

/**
 * Register for relevant components of this module.
 */
public class Tensorflow {

    private final static TensorflowPlugin PLUGIN = new TensorflowPlugin();

    private final static TensorflowConversionPlugin CONVERSION_PLUGIN = new TensorflowConversionPlugin();

    /**
     * Retrieve the {@link TensorflowPlugin}.
     *
     * @return the {@link TensorflowPlugin}
     */
    public static TensorflowPlugin plugin() {
        return PLUGIN;
    }

    /**
     * Retrieve the {@link TensorflowConversionPlugin}.
     *
     * @return the {@link TensorflowConversionPlugin}
     */
    public static TensorflowConversionPlugin channelConversionPlugin() {
        return CONVERSION_PLUGIN;
    }

    /**
     * Retrieve the {@link TensorflowPlatform}.
     *
     * @return the {@link TensorflowPlatform}
     */
    public static TensorflowPlatform platform() {
        return TensorflowPlatform.getInstance();
    }
}
