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

public class SimpleSinglePlan extends LogicalPlan {

  private List<SQLParser.ResultColumnContext> resultColumns;

  private String tableName;

  private SQLParser.ConditionContext condition;

  public List<SQLParser.ResultColumnContext> getResultColumns() {
    return resultColumns;
  }

  public String getTableName() {
    return tableName;
  }

  public SQLParser.ConditionContext getCondition() {
    return condition;
  }

  public SimpleSinglePlan(
      List<SQLParser.ResultColumnContext> resultColumnContext,
      String tableName,
      SQLParser.ConditionContext conditionContext) {
    super(LogicalPlanType.SIMPLE_SELECT_SINGLE_TABLE);
    this.resultColumns = resultColumnContext;
    this.tableName = tableName;
    this.condition = conditionContext;
  }

  @Override
  public String toString() {
    StringBuilder res = new StringBuilder("ResultColumn: ");
    for (SQLParser.ResultColumnContext resultColumn : resultColumns) {
      res.append(resultColumn.getText());
    }
    res.append("\nTableQuery:" + tableName);
    res.append("\nMultipleCondition:").append(condition == null ? "" : condition.getText());
    return "SimpleSinglePlan{\n" + res.toString() + "\n}";
  }
}
