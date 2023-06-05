package cn.edu.thssdb.schema;

import cn.edu.thssdb.query.QueryTable2;
import cn.edu.thssdb.type.ColumnType;

import java.io.Serializable;

public class Column implements Comparable<Column>, Serializable {
  private String name;
  private ColumnType type;
  private final int primary;
  private final boolean notNull;
  private final int maxLength;
  private Table table;
  private QueryTable2 queryTable;

  public Column(String name, ColumnType type, int primary, boolean notNull, int maxLength) {
    this.name = name;
    this.type = type;
    this.primary = primary;
    if (primary != 0) {
      this.notNull = true;
    } else {
      this.notNull = notNull;
    }
    this.maxLength = maxLength;
    this.table = null;
    this.queryTable = null;
  }

  public Column clone() {
    Column column = new Column(name, type, primary, notNull, maxLength);
    column.setTable(table);
    column.setQueryTable(queryTable);
    return column;
  }

  public void setTable(Table table) {
    this.table = table;
  }

  public void setQueryTable(QueryTable2 queryTable) {
    this.queryTable = queryTable;
  }

  public String getTableName() {
    if (table != null) {
      return table.getTableName();
    }
    if (queryTable != null) {
      return queryTable.getQueryName();
    }
    return null;
  }

  public int getIndex() {
    //    System.out.println("getIndex: "+table.getTableName()+" "+queryTable.getQueryName()+"
    // "+name);
    if (queryTable != null) {
      return queryTable.getColumns().indexOf(this);
    }
    //    if (table != null) {
    //      return table.getColumns().indexOf(this);
    //    }
    return -1;
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
