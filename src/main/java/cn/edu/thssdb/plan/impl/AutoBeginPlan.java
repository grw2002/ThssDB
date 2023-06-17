package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;

public class AutoBeginPlan extends LogicalPlan {
  public AutoBeginPlan() {
    super(LogicalPlanType.AUTO_BEGIN_TRANSACTION);
  }

  @Override
  public String toString() {
    return "AutoBeginTransactionPlan{}";
  }
}
