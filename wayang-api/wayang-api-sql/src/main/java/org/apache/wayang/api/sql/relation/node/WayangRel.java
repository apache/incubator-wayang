/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.wayang.api.sql.relation.node;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;

import java.util.List;
import java.util.stream.Stream;

/**
 * Relational expression that uses the Wayang calling convention.
 */
public interface WayangRel extends RelNode {
  /**
   * Converts this node to a Pig Latin statement.
   */
  void implement(Implementor implementor);

  /** Calling convention for relational operations that occur in Wayang. */
  Convention CONVENTION = new Convention.Impl("Wayang", WayangRel.class);

//    @Override
//    default RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
//        return planner.getCostFactory().makeZeroCost();
//    }

  /**
   * Callback for the implementation process that converts a tree of
   * {@link WayangRel} nodes into complete Wayang Plan.
   */
  class Implementor {

    private Stream<String> statements;

    public String getTableName(RelNode input) {
      final List<String> qualifiedName = input.getTable().getQualifiedName();
      return qualifiedName.get(qualifiedName.size() - 1);
    }

    public String getWayangRelationAlias(RelNode input) {
      return getTableName(input);
    }

    public String getFieldName(RelNode input, int index) {
      return input.getRowType().getFieldList().get(index).getName();
    }

    public void addStatement(Stream<String> statement) {
      statements = statement;
    }

    public void visitChild(int ordinal, RelNode input) {
      assert ordinal == 0;
      ((WayangRel) input).implement(this);
    }

    public Stream<String> getStatements() {
      return statements;
    }
  }

}
