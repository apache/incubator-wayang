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

package org.apache.wayang.api

import org.apache.wayang.basic.data.{Tuple2 => T2}

/**
  * Provides implicits for Wayang's graph API.
  */
package object graph {

  /**
   * TODO: add the description for the implicitis in org.apache.wayang.api.graph
   * labels: documentation,todo
   */

  type Vertex = java.lang.Long

  type Edge = T2[Vertex, Vertex]

  type PageRank = T2[Vertex, java.lang.Float]

  implicit def elevateEdgeDataQuanta(dataQuanta: DataQuanta[Edge]): EdgeDataQuanta =
    new EdgeDataQuanta(dataQuanta)

}
