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
package org.apache.wayang.api.json.operatorfromdrawflow

import org.apache.wayang.api.json.operatorfromdrawflow.OperatorFromDrawflow.{InputConnections, OutputConnections}

case class OperatorFromDrawflow(id: Long,
                                name: String,
                                inputs: Map[String, InputConnections],
                                outputs: Map[String, OutputConnections],
                                data: Map[String, Any]) {
}

object OperatorFromDrawflow {
  case class InputConnection(input: String, node: String) {}
  case class OutputConnection(output: String, node: String) {}
  case class InputConnections(connections: List[InputConnection]) {}
  case class OutputConnections(connections: List[OutputConnection]) {}
}

