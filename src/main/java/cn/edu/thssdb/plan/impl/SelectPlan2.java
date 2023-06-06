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

public class SelectPlan2 extends LogicalPlan {

  List<SQLParser.ResultColumnContext> resultColumns;

  List<SQLParser.TableQueryContext> tableQuerys;

  SQLParser.MultipleConditionContext conditions;

  List<String> columnNames;

  public List<SQLParser.ResultColumnContext> getResultColumns() {
    return resultColumns;
  }

  public List<SQLParser.TableQueryContext> getTableQuerys() {
    return tableQuerys;
  }

  public SQLParser.MultipleConditionContext getMultipleCondition() {
    return conditions;
  }

  public SelectPlan2(
      List<SQLParser.ResultColumnContext> resultColumnContext,
      List<SQLParser.TableQueryContext> tableQueryContext,
      SQLParser.MultipleConditionContext multipleConditionContext) {
    super(LogicalPlanType.SELECT_FROM_TABLE);
    this.resultColumns = resultColumnContext;
    this.tableQuerys = tableQueryContext;
    this.conditions = multipleConditionContext;
  }

  @Override
  public String toString() {
    StringBuilder res = new StringBuilder("ResultColumn: ");
    for (SQLParser.ResultColumnContext resultColumn : resultColumns) {
      res.append(resultColumn.getText());
    }
    res.append("\nTableQuery:");
    for (SQLParser.TableQueryContext tableQuery : tableQuerys) {
      res.append(tableQuery.getText());
    }
    res.append("\nMultipleCondition:").append(conditions == null ? "" : conditions.getText());
    return "SelectPlan{\n" + res.toString() + "\n}";
  }
}
