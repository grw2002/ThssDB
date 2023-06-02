package cn.edu.thssdb.schema;

import java.io.Serializable;

public class ForeignKeyConstraint implements Serializable {

  private String foreignKeyName;
  private String referencedTableName;
  private String referencedColumnName;

  public ForeignKeyConstraint(
      String foreignKeyName, String referencedTableName, String referencedColumnName) {
    this.foreignKeyName = foreignKeyName;
    this.referencedTableName = referencedTableName;
    this.referencedColumnName = referencedColumnName;
  }

  public String getForeignKeyName() {
    return foreignKeyName;
  }

  public String getReferencedTableName() {
    return referencedTableName;
  }

  public String getReferencedColumnName() {
    return referencedColumnName;
  }

  // Check if the constraint is satisfied for the given value.
  // You might need to fetch the referenced table and column in your database to perform this check.
  public boolean isSatisfied(Object value) {
    // TODO: Implement this method
    return true;
  }
}
