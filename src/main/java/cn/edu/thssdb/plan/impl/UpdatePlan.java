package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;

import java.util.List;

public class UpdatePlan extends LogicalPlan {
  private final String tableName;
  private final String columnName;
  private final String newValue;
  private final List<String> conditions;

  public UpdatePlan(String tableName, String columnName, String newValue, List<String> conditions) {
    super(LogicalPlanType.UPDATE_TABLE);
    this.tableName = tableName;
    this.columnName = columnName;
    this.newValue = newValue;
    this.conditions = conditions;
  }

  public String getTableName() {
    return tableName;
  }

  public String getColumnName() {
    return columnName;
  }

  public String getNewValue() {
    return newValue;
  }

  public List<String> getConditions() {
    return conditions;
  }

  @Override
  public String toString() {
    return "InsertPlan{"
        + "tableName='"
        + tableName
        + "' "
        + columnName
        + "="
        + newValue
        + " WHERE "
        + conditions.toString()
        + '}';
  }
}
