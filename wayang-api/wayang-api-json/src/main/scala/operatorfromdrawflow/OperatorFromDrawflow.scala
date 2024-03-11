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

