package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.utils.Cell;
import cn.edu.thssdb.utils.Pair;

import java.util.*;

public class QueryResult {

  public List<MetaInfo> metaInfos;
  public List<QueryTable> queryTables;
  public List<Integer> index;
  private List<Cell> attrs;

  public static Pair<List<String>, List<List<String>>> makeResult(QueryResult queryResult) {
    List<LinkedList<Row>> allRows = queryResult.getMatchRows();
    List<LinkedList<Row>> filteredRows = QueryResult.filterColumns(allRows, queryResult.metaInfos);
    for (LinkedList<Row> filteredRow : filteredRows) {
      System.out.println(filteredRow.toString());
    }
    List<String> left = new ArrayList<>();
    List<List<String>> right = new ArrayList<>();
    for (LinkedList<Row> filteredRow : filteredRows) {
      right.add(rowToStringList(QueryResult.combine(filteredRow)));
    }
    for (MetaInfo metaInfo : queryResult.metaInfos) {
      for (Column column : metaInfo.getColumns()) {
        left.add(column.getName());
      }
    }
    return new Pair<>(left, right);
  }

  public static List<String> rowToStringList(Row row) {
    List<String> result = new ArrayList<>();
    for (Entry entry : row.getEntries()) {
      result.add(entry.toString());
    }
    return result;
  }

  public QueryResult(List<QueryTable> queryTables, List<MetaInfo> metaInfos) {
    // TODO
    this.metaInfos = metaInfos;
    this.queryTables = queryTables;
    this.index = new ArrayList<>();
    this.attrs = new ArrayList<>();
    //    this.result = new Pair<>(new ArrayList<>(), new ArrayList<>());
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

  // TODO: Add Match Rules
  public List<LinkedList<Row>> getMatchRows() {
    // Cartesion Product
    List<LinkedList<Row>> oldrows = new LinkedList<>();
    List<LinkedList<Row>> newrows = new LinkedList<>();
    List<LinkedList<Row>> tmp = null;
    oldrows.add(new LinkedList<>());
    for (QueryTable queryTable : queryTables) {
      while (queryTable.hasNext()) {
        Row row = queryTable.next();
        for (LinkedList<Row> oldrow : oldrows) {
          LinkedList<Row> newRow = new LinkedList<>(oldrow);
          newRow.add(row);
          newrows.add(newRow);
        }
      }
      // Swap
      tmp = oldrows;
      oldrows = newrows;
      newrows = tmp;
      newrows.clear();
    }
    return (LinkedList) oldrows;
  }

  public static Row combine(LinkedList<Row> rows) {
    List<Entry> entries = new ArrayList<>();
    for (Row row : rows) {
      entries.addAll(row.getEntries());
    }
    return new Row(entries.toArray(new Entry[entries.size()]));
  }

  // choose columns that metainfo contains
  public static List<LinkedList<Row>> filterColumns(
      List<LinkedList<Row>> allrows, List<MetaInfo> metaInfos) {
    //    for (MetaInfo metaInfo : metaInfos) {
    //      System.out.println(metaInfo.getTableName() + metaInfo.getColumns().toString());
    //    }
    List<LinkedList<Row>> result = new ArrayList<>();
    for (LinkedList<Row> rows : allrows) {
      Iterator<Row> rowIter = rows.iterator();
      Iterator<MetaInfo> metaInfoIterator = metaInfos.iterator();
      LinkedList<Row> newRows = new LinkedList<>();
      while (rowIter.hasNext()) {
        Row row = rowIter.next();
        MetaInfo metaInfo = metaInfoIterator.next();
        List<Entry> entries = row.getEntries();
        Entry[] newEntries = new Entry[metaInfo.getColumns().size()];
        for (int i = 0; i < metaInfo.getColumns().size(); i++) {
          Column column = metaInfo.getColumns().get(i);
          newEntries[i] = entries.get(column.getIndex());
          //          System.out.println(column.getName() + column.getIndex() + entries.toString());
        }
        newRows.add(new Row(newEntries));
      }
      result.add(newRows);
    }
    return result;
  }

  public Row generateQueryRecord(Row row) {
    // TODO
    return null;
  }
}
