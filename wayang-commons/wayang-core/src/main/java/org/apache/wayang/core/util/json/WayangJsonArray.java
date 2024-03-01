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

package org.apache.wayang.core.util.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.StreamSupport;
import org.apache.wayang.core.api.exception.WayangException;

/**
 * JSONArray is the wrapper for the {@link ArrayNode} to enable the
 * easy access to Json data
 *
 * TODO: the java doc is not done because is missing implementation and it performed some
 *       modification on the code
 */
public class WayangJsonArray implements Iterable<Object>{

  private ArrayNode node;

  public WayangJsonArray(){
    ObjectMapper mapper = new ObjectMapper();
    // create a JSON object
    this.node = mapper.createArrayNode();
  }

  WayangJsonArray(ArrayNode node){
    this.node = node;
  }

  ArrayNode getNode(){
    return this.node;
  }

  public int length(){
    return this.node.size();
  }

  public WayangJsonObj getJSONObject(int index){
    return new WayangJsonObj((ObjectNode) this.node.get(index));
  }

  public void put(WayangJsonObj value){
    this.node.add(value.getNode());
  }

  public void put(Object value){
    this.insertType(value).accept(value);
  }

  Consumer<Object> insertType(Object value){
    ArrayNode node = this.getNode();
    if(value == null){
      return (v) ->  node.add(WayangJsonObj.NULL);
    }else if(value instanceof Integer){
      return (v) ->  node.add((Integer) v);
    }else if(value instanceof Long){
      return (v) ->  node.add((Long) v);
    }else if(value instanceof Float){
      return (v) ->  node.add((Float) v);
    }else if(value instanceof String){
      return (v) ->  node.add((String) v);
    }else if(value instanceof Double){
      return (v) ->  node.add((Double) v);
    }else if(value instanceof Boolean){
      return (v) ->  node.add((Boolean) v);
    }else if(value instanceof JsonNode){
      return (v) -> node.add((JsonNode) v);
    }else if(value instanceof WayangJsonArray){
      return (v) -> node.add(((WayangJsonArray)v).getNode());
    }else if(value instanceof WayangJsonObj){
      return (v) -> node.add(((WayangJsonObj)v).getNode());
    }
    throw new WayangException("The type is not recognizable "+ value.getClass());
  }

  @Override
  public String toString() {
    return this.getNode().toString();
  }

  @Override
  public Iterator<Object> iterator() {
    return StreamSupport.stream(
        Spliterators.spliteratorUnknownSize(this.getNode().iterator(), Spliterator.ORDERED),
        false)
        .map(v -> {
          if(v instanceof ArrayNode){
            return new WayangJsonArray((ArrayNode) v);
          }else if(v instanceof ObjectNode) {
            return new WayangJsonObj((ObjectNode) v);
          }else if(v instanceof NullNode) {
            return null;
          } else if(v instanceof TextNode){
            return ((TextNode)v).asText();
          }else if(v instanceof DoubleNode){
            return ((DoubleNode)v).asDouble();
          }else if(v instanceof IntNode){
            return ((IntNode)v).asInt();
          }else if(v instanceof LongNode){
            return ((LongNode)v).asLong();
          }

          else{
            throw new WayangException("the object type is not valid "+ v.getClass());
          }
        })
        .iterator();
  }
}
