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

package org.apache.wayang.api.util

import org.apache.wayang.core.types.DataSetType

/**
  * This class waits for a [[org.apache.wayang.core.types.DataSetType]] to be set and verifies that there are no
  * two different sets.
  */
class TypeTrap {

  /**
    * Stores the [[DataSetType]].
    */
  private var _dataSetType: DataSetType[_] = _

  /**
    * Set the [[DataSetType]] for this instance.
    *
    * @throws IllegalArgumentException if a different [[DataSetType]] has been set before
    * @param dst the [[DataSetType]]
    */
  def dataSetType_=(dst: DataSetType[_]): Unit = {
    _dataSetType match {
      case null => _dataSetType = dst
      case `dst` =>
      case other => throw new IllegalArgumentException(s"Conflicting types ${_dataSetType} and ${dst}.")
    }
  }

  /**
    * Return the previously set [[DataSetType]].
    *
    * @return the previously set [[DataSetType]] or, if none has been set, [[DataSetType.none()]]
    */
  def dataSetType = _dataSetType match {
    case null => DataSetType.none()
    case other => other
  }

  /**
    * Return the [[Class]] of the previously set [[DataSetType]].
    *
    * @return the [[Class]] of the previously set [[DataSetType]] or, if none has been set, that of [[DataSetType.none()]]
    */
  def typeClass = dataSetType.getDataUnitType.toBasicDataUnitType.getTypeClass
}
