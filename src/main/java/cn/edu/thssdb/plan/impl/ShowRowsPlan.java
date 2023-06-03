package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;

public class ShowRowsPlan extends LogicalPlan {
  private String tableName;

  public ShowRowsPlan(String tableName) {
    super(LogicalPlanType.SHOW_ROWS);
    this.tableName = tableName;
  }

  public String getTableName() {
    return tableName;
  }

  @Override
  public String toString() {
    return "ShowRowsPlan{" + "tableName='" + tableName + '\'' + '}';
  }
}
