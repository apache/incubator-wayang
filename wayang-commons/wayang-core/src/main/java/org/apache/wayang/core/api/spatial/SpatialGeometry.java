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

package org.apache.wayang.core.api.spatial;

import java.io.Serializable;

/**
 * Abstract geometry interface for spatial operations.
 * Implementations (e.g., WayangGeometry) provide JTS-backed functionality.
 */
public interface SpatialGeometry extends Serializable {

    /**
     * Returns Well-Known Text (WKT) representation of this geometry.
     *
     * @return WKT string
     */
    String toWKT();

    /**
     * Returns Well-Known Binary (WKB) representation of this geometry as hex string.
     *
     * @return WKB hex string
     */
    String toWKB();
}
