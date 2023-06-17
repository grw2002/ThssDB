package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Column;

import java.util.*;

public class MetaInfo2 {

  protected List<Column> columns;
  protected transient Map<String, Integer> columnIndex;

  public List<Column> getColumns() {
    return columns;
  }

  public MetaInfo2(List<Column> columns) {
    this.columns = new ArrayList<>();
    for (Column column : columns) {
      this.columns.add(column.clone());
    }
  }

  protected void updateColumnIndex() {
    columnIndex.clear();
    for (int i = 0; i < columns.size(); i++) {
      columnIndex.put(columns.get(i).getName(), i);
    }
  }

  public void addColumn(Column column) {
    this.columns.add(column);
    updateColumnIndex();
  }

  @Deprecated
  public int columnFind(String name) {
    for (int i = 0; i < columns.size(); i++) {
      if (columns.get(i).getName().equals(name)) {
        return i;
      }
    }
    return -1;
  }

  public Column findColumnByName(String name) {
    for (Column column : columns) {
      if (column.getName().equals(name)) {
        return column;
      }
    }
    return null;
  }

  public int findColumnIndexByName(String name) {
    //    System.out.println("findColumnIndexByName: " + name);
    Column column = findColumnByName(name);
    if (column != null) {
      return column.getIndex();
    }
    return -1;
  }

  public Column findColumnByName(String name, String tableName) {
    for (Column column : columns) {
      if (column.getName().equals(name)) {
        if (column.getTableName().equals(tableName)) {
          return column;
        }
      }
    }
    return null;
  }

  public int findColumnIndexByName(String name, String tableName) {
    Column column = findColumnByName(name, tableName);
    if (column != null) {
      return column.getIndex();
    }
    return -1;
  }
}
