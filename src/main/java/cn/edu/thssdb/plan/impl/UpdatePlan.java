package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.sql.SQLParser;

public class UpdatePlan extends LogicalPlan {
  private final String tableName;
  private final String columnName;
  //  private final String newValue;
  //  private final List<String> conditions;
  private final SQLParser.LiteralValueContext newValue;
  private final SQLParser.ConditionContext condition;

  public UpdatePlan(
      String tableName,
      String columnName,
      SQLParser.LiteralValueContext newValue,
      SQLParser.ConditionContext condition) {
    super(LogicalPlanType.UPDATE_TABLE);
    this.tableName = tableName;
    this.columnName = columnName;
    this.newValue = newValue;
    this.condition = condition;
  }

  public String getTableName() {
    return tableName;
  }

  public String getColumnName() {
    return columnName;
  }

  public SQLParser.LiteralValueContext getNewValue() {
    return newValue;
  }

  public SQLParser.ConditionContext getCondition() {
    return condition;
  }

  @Override
  public String toString() {
    return "InsertPlan{"
        + "tableName='"
        + tableName
        + "' "
        + columnName
        + "="
        + newValue.getText()
        + " WHERE "
        + condition.getText()
        + '}';
  }
}
