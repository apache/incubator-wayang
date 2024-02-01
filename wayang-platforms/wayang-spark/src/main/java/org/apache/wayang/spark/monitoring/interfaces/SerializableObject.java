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

package org.apache.wayang.spark.monitoring.interfaces;

import java.io.Serializable;
/**
 * The {@code SerializableObject} interface is a marker interface that indicates
 * that its implementing classes are serializable. By extending the {@link Serializable}
 * interface, classes that implement this interface can be serialized and deserialized
 * to and from a stream of bytes.
 * <p>
 * It is recommended that classes implementing this interface provide a serialVersionUID
 * field to ensure compatibility between serialized objects of different versions.
 * </p>
 * <p>
 * This interface does not define any methods, but instead serves as a tag to indicate
 * that implementing classes can be serialized.
 * </p>
 *
 * @see Serializable
 */
public interface SerializableObject extends Serializable {
}
