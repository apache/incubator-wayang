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
package org.apache.wayang.api.json.operatorfromjson.binary

import com.fasterxml.jackson.annotation.JsonTypeName
import org.apache.wayang.api.json.operatorfromjson.OperatorFromJson
import org.apache.wayang.api.util.NDimArray
import org.apache.wayang.basic.model.op.{Op => ModelOp};
import org.apache.wayang.basic.model.optimizer.{Optimizer => ModelOptimizer};
import org.apache.wayang.basic.model.optimizer._;
import org.apache.wayang.basic.model.op.nn._;
import org.apache.wayang.basic.model.op._;

@JsonTypeName(OperatorFromJson.OperatorNames.DLTraining)
case class DLTrainingOperatorFromJson(override val id: Long,
                                override val input: Array[Long],
                                override val output: Array[Long],
                                override val cat: String,
                                override val operatorName: String,
                                val data: DLTrainingData,
                                override val executionPlatform: String = null)
  extends OperatorFromJson(id, input, output, cat, operatorName, executionPlatform) {
}
case class Op(val op: String, val opType: String, val dType: String, val fromList: Array[Op], val dim: Integer, val labels: Integer, val inFeatures: Integer, val outFeatures: Integer, val bias: Boolean){
  def toModelOp(): ModelOp = {
    val recursiveOp: ModelOp = op match {
      case "ArgMax" => new ArgMax(dim)
      case "Cast" => new Cast(parseDType(dType))
      case "CrossEntropyLoss" => new CrossEntropyLoss(labels)
      case "Eq" => new Eq()
      case "Input" => new Input(parseInputType(opType))
      case "Mean" => new Mean(dim)
      case "Linear" => new Linear(inFeatures, outFeatures, bias)
      case "ReLU" => new ReLU()
      case "Sigmoid" => new Sigmoid()
      case "Softmax" => new Softmax()
    }

    val children = fromList.map(child => child.toModelOp())
    children.foreach(child => recursiveOp.`with`(child))

    recursiveOp
  }

  private def parseDType(dType: String): ModelOp.DType = {
    dType match {
      case "ANY" => ModelOp.DType.ANY
      case "INT32" => ModelOp.DType.INT32
      case "INT64" => ModelOp.DType.INT64
      case "FLOAT32" => ModelOp.DType.FLOAT32
      case "FLOAT64" => ModelOp.DType.FLOAT64
      case "BYTE" => ModelOp.DType.BYTE
      case "INT16" => ModelOp.DType.INT16
      case "BOOL" => ModelOp.DType.BOOL
    }
  }

  private def parseInputType(inputType: String): Input.Type = {
    inputType match {
      case "..FEATURES.." => Input.Type.FEATURES
      case "..LABEL.." => Input.Type.LABEL
      case "..PREDICTED.." => Input.Type.PREDICTED
    }
  }
}

case class Optimizer(val name: String, val learningRate: Float){
  def toModelOptimizer(): ModelOptimizer = {
    name match {
      case "Adam" => new Adam(learningRate)
      case "GradientDescent" => new GradientDescent(learningRate)
    }
  }
}

case class Model(val modelType: String, val op: Op){}

case class Option(val optimizer: Optimizer, val criterion: Op, val batchSize: Long, val epoch: Long){}

case class DLTrainingData(val model: Model, val option: Option, val inputType: scala.Option[NDimArray], val outputType: scala.Option[NDimArray]){}
