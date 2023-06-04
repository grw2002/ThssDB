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
import cn.edu.thssdb.query.MetaInfo;
import cn.edu.thssdb.query.QueryTable;

import java.util.List;

public class SelectPlan extends LogicalPlan {

  List<QueryTable> queryTables;
  List<MetaInfo> metaInfos;
  //  QueryResult pendingQuery;
  String joinCondition;

  public List<QueryTable> getQueryTables() {
    return queryTables;
  }

  public List<MetaInfo> getMetaInfos() {
    return metaInfos;
  }

  public String getJoinCondition() {
    return joinCondition;
  }

  public SelectPlan(List<QueryTable> queryTables, List<MetaInfo> metaInfos, String joinCondition) {
    super(LogicalPlanType.SELECT_FROM_TABLE);
    this.queryTables = queryTables;
    this.metaInfos = metaInfos;
    this.joinCondition = joinCondition;
  }

  @Override
  public String toString() {
    return "SelectPlan{" + "databaseName='" + metaInfos.toString() + '\'' + '}';
  }
}
