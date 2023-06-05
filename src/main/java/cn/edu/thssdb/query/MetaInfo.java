package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Column;

import java.util.ArrayList;
import java.util.List;

@Deprecated
public class MetaInfo {

  private String tableName;
  private List<Column> columns;

  public MetaInfo(String tableName, ArrayList<Column> columns) {
    this.tableName = tableName;
    this.columns = columns;
  }

  public String getTableName() {
    return tableName;
  }

  public List<Column> getColumns() {
    return columns;
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
}
