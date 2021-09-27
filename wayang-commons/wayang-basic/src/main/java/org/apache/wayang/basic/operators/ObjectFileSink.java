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

package org.apache.wayang.basic.operators;

import java.util.Objects;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.optimizer.costs.DefaultLoadEstimator;
import org.apache.wayang.core.optimizer.costs.NestableLoadProfileEstimator;
import org.apache.wayang.core.plan.wayangplan.UnarySink;
import org.apache.wayang.core.types.DataSetType;

/**
 * This {@link UnarySink} writes all incoming data quanta to a Object file.
 *
 * @param <T> type of the object to store
 */
public class ObjectFileSink<T> extends UnarySink<T> {

  protected final String textFileUrl;

  protected final Class<T> tClass;

  /**
   * Creates a new instance.
   *
   * @param targetPath  URL to file that should be written
   * @param type        {@link DataSetType} of the incoming data quanta
   */
  public ObjectFileSink(String targetPath, DataSetType<T> type) {
    super(type);
    this.textFileUrl = targetPath;
    this.tClass = type.getDataUnitType().getTypeClass();
  }

  /**
   * Creates a new instance.
   *
   * @param textFileUrl        URL to file that should be written
   * @param typeClass          {@link Class} of incoming data quanta
   */
  public ObjectFileSink(String textFileUrl, Class<T> typeClass) {
    super(DataSetType.createDefault(typeClass));
    this.textFileUrl = textFileUrl;
    this.tClass = typeClass;
  }

  /**
   * Creates a copied instance.
   *
   * @param that should be copied
   */
  public ObjectFileSink(ObjectFileSink<T> that) {
    super(that);
    this.textFileUrl = that.textFileUrl;
    this.tClass = that.tClass;
  }
}
