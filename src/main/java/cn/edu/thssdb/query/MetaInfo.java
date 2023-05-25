package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Column;

import java.util.ArrayList;
import java.util.List;

public class MetaInfo {

  private String tableName;
  private List<Column> columns;

  public MetaInfo(String tableName, ArrayList<Column> columns) {
    this.tableName = tableName;
    this.columns = columns;
  }

  public List<Column> getColumns() {
    return columns;
  }

  public int columnFind(String name) {
    for (int i = 0; i < columns.size(); i++) {
      if (columns.get(i).getName() == name) {
        return i;
      }
    }
    return -1;
  }
}
