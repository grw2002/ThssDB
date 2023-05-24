package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.utils.Cell;
import cn.edu.thssdb.utils.Pair;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class QueryResult {

  public List<MetaInfo> metaInfos;
  public QueryTable[] queryTables;
  public List<Integer> index;
  private List<Cell> attrs;
  public Pair<List<Column>, List<Row>> result;

  public QueryResult(QueryTable[] queryTables, List<MetaInfo> metaInfos) {
    // TODO
    this.metaInfos = metaInfos;
    this.queryTables = queryTables;
    this.index = new ArrayList<>();
    this.attrs = new ArrayList<>();
    this.result = new Pair<>(new ArrayList<>(), new ArrayList<>());
  }

  public List<String> getColumnsList() {
    List<String> columns = new ArrayList<>();
    for (MetaInfo metaInfo : metaInfos) {
      for (Column column : metaInfo.getColumns()) {
        columns.add(column.getName());
      }
    }
    return columns;
  }

  public List<List<String>> getRowsList() {
    return new ArrayList<>();
  }

  public static Row combineRow(LinkedList<Row> rows) {
    // TODO
    return null;
  }

  public static Row filterRow(Row row, List<Integer> index) {
    return row;
  }

  public Row generateQueryRecord(Row row) {
    // TODO
    return null;
  }
}
