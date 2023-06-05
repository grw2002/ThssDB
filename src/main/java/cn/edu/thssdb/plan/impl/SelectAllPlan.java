package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;

public class SelectAllPlan extends LogicalPlan {
  private String tableName;

  public SelectAllPlan(String tableName) {
    super(LogicalPlanType.SELECT_ALL);
    this.tableName = tableName;
  }

  public String getTableName() {
    return tableName;
  }

  @Override
  public String toString() {
    return "SelectAllPlan{" + "tableName='" + tableName + '\'' + '}';
  }
}
