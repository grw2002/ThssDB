package cn.edu.thssdb.schema;

import cn.edu.thssdb.type.ColumnType;

import java.io.Serializable;

public class Column implements Comparable<Column>, Serializable {
  private String name;
  private ColumnType type;
  private final int primary;
  private final boolean notNull;
  private final int maxLength;
  private Table table;

  public Column(String name, ColumnType type, int primary, boolean notNull, int maxLength) {
    this.name = name;
    this.type = type;
    this.primary = primary;
    this.notNull = notNull;
    this.maxLength = maxLength;
    this.table = null;
  }

  public void setTable(Table table) {
    this.table = table;
  }

  public int getIndex() {
    if (table == null) {
      return -1;
    }
    return table.getColumns().indexOf(this);
  }

  public int getPrimary() {
    return primary;
  }

  public String getName() {
    return name;
  }

  public ColumnType getType() {
    return type;
  }

  public boolean getNotNull() {
    return notNull;
  }

  public int getMaxLength() {
    return maxLength;
  }

  public void setType(String newColumnType) {
    this.type = ColumnType.valueOf(newColumnType);
  }

  public void setName(String newColumnName) {
    this.name = newColumnName;
  }

  @Override
  public int compareTo(Column e) {
    return name.compareTo(e.name);
  }

  public String toString() {
    return name + ',' + type + ',' + primary + ',' + notNull + ',' + maxLength;
  }
}
