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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.sql.SQLParser;

import java.util.List;

public class SimpleJoinPlan extends LogicalPlan {

  private List<SQLParser.ResultColumnContext> resultColumns;

  private String tableL, tableR;

  private SQLParser.ConditionContext joinCondition;

  private SQLParser.ConditionContext whereCondition;

  public List<SQLParser.ResultColumnContext> getResultColumns() {
    return resultColumns;
  }

  public String getTableL() {
    return tableL;
  }

  public String getTableR() {
    return tableR;
  }

  public SQLParser.ConditionContext getJoinCondition() {
    return joinCondition;
  }

  public SQLParser.ConditionContext getWhereCondition() {
    return whereCondition;
  }

  public SimpleJoinPlan(
      List<SQLParser.ResultColumnContext> resultColumnContext,
      String tableL,
      String tableR,
      SQLParser.ConditionContext joinCondition,
      SQLParser.ConditionContext whereCondition) {
    super(LogicalPlanType.SIMPLE_SELECT_JOIN_TABLE);
    this.resultColumns = resultColumnContext;
    this.tableL = tableL;
    this.tableR = tableR;
    this.joinCondition = joinCondition;
    this.whereCondition = whereCondition;
  }

  @Override
  public String toString() {
    StringBuilder res = new StringBuilder("ResultColumn: ");
    for (SQLParser.ResultColumnContext resultColumn : resultColumns) {
      res.append(resultColumn.getText());
    }
    res.append("\nTableQuery:" + tableL + " " + tableR);
    res.append("\njoinConditions:").append(joinCondition == null ? "" : joinCondition.getText());
    res.append("\nwhereConditions:").append(whereCondition == null ? "" : whereCondition.getText());
    return "SimpleJoinPlan{\n" + res.toString() + "\n}";
  }
}
