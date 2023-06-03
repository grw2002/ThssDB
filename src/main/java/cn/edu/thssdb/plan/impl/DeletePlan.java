package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;

import java.util.List;

public class DeletePlan extends LogicalPlan {
  private String tableName;
  private List<String> conditions;

  public DeletePlan(String tableName, List<String> conditions) {
    super(LogicalPlanType.DELETE_FROM_TABLE);
    this.tableName = tableName;
    this.conditions = conditions;
  }

  public String getTableName() {
    return this.tableName;
  }

  public List<String> getConditions() {
    return this.conditions;
  }

  @Override
  public String toString() {
    return "InsertPlan{" + "tableName='" + tableName + '\'' + conditions.toString() + '}';
  }
}
