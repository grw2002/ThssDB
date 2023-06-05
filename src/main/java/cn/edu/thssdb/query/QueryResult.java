package cn.edu.thssdb.query;

import cn.edu.thssdb.exception.ColumnNotExistException;
import cn.edu.thssdb.exception.DatabaseNotExistException;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.utils.Cell;
import cn.edu.thssdb.utils.Pair;

import java.util.*;

@Deprecated
public class QueryResult {

  public List<MetaInfo> metaInfos;
  public List<QueryTable> queryTables;
  public List<Integer> index;
  private List<Cell> attrs;
  private String joinCondition;
  private String whereCondition;

  Manager manager;

  public QueryResult(
      List<QueryTable> queryTables,
      List<MetaInfo> metaInfos,
      String joinCondition,
      String whereCondition) {
    // TODO
    this.metaInfos = metaInfos;
    this.queryTables = queryTables;
    this.index = new ArrayList<>();
    this.attrs = new ArrayList<>();
    this.joinCondition = joinCondition;
    this.whereCondition = whereCondition;
    this.manager = Manager.getInstance();
    // this.result = new Pair<>(new ArrayList<>(), new ArrayList<>());
  }

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
    List<LinkedList<Row>> tmp;
    oldrows.add(new LinkedList<>());
    Database currentDb = manager.getCurrentDatabase();
    if (currentDb == null) {
      throw new DatabaseNotExistException();
    }
    for (int i = 0; i < queryTables.size(); i++) {
      QueryTable queryTable = queryTables.get(i);
      MetaInfo metaInfo = metaInfos.get(i);
      Table table = queryTable.table;

      while (queryTable.hasNext()) {
        Row row = queryTable.next();
        boolean joinConditionSatisfied = true;
        boolean whereConditionSatified = true;

        //        if (joinCondition != null) {
        //          // Parse join condition
        //          String[] parts = joinCondition.split("=");
        //          String[] leftPart = parts[0].split("\\.");
        //          String[] rightPart = parts[1].split("\\.");
        //
        //          String leftTableName = leftPart[0];
        //          String leftAttrName = leftPart[1];
        //          String rightTableName = rightPart[0];
        //          String rightAttrName = rightPart[1];
        ////
        //          // Check join condition
        //          for (MetaInfo metaInfo1 : metaInfos) {
        //            if (metaInfo1.getTableName().equals(leftTableName)) {
        //              Column column = table.findColumnByName(leftAttrName);
        //              if (column != null) {
        //                int leftIndex = column.getIndex();
        //                Entry leftEntry = row.getEntries().get(leftIndex);
        //                if (metaInfo1.getTableName().equals(rightTableName)) {
        //                  column = metaInfo1.findColumnByName(rightAttrName);
        //                  if (column != null) {
        //                    int rightIndex = column.getIndex();
        //                    Entry rightEntry = row.getEntries().get(rightIndex);
        //                    if (!leftEntry.equals(rightEntry)) {
        //                      joinConditionSatisfied = false;
        //                      break;
        //                    }
        //                  } else {
        //                    throw new ColumnNotExistException(rightAttrName);
        //                  }
        //                }
        //              } else {
        //                throw new ColumnNotExistException(leftAttrName);
        //              }
        //            }
        //          }
        //        }

        if (whereCondition != null) {
          // Parse where condition
          //          System.out.println(whereCondition);
          String[] parts = whereCondition.split(" ");
          String conditionColumnName = parts[0];
          String operator = parts[1];
          String value = parts[2];
          if (value.startsWith("'") && value.endsWith("'")) {
            value = value.substring(1, value.length() - 1);
          }

          // Get the value in the column for this row

          if (table.findColumnIndexByName(conditionColumnName) == -1) {
            throw new ColumnNotExistException(conditionColumnName);
          }
          Entry columnValueEntry =
              row.getEntries().get(table.findColumnIndexByName(conditionColumnName));
          String columnValue = columnValueEntry.toString();

          // Check if the condition is satisfied for this row
          switch (operator) {
            case "=":
              if (!columnValue.equals(value)) {
                whereConditionSatified = false;
              }
              break;
            case "<>":
              if (columnValue.equals(value)) {
                whereConditionSatified = false;
              }
              break;
            case "<":
              if (!(columnValue.compareTo(value) < 0)) {
                whereConditionSatified = false;
              }
              break;
            case ">":
              if (!(columnValue.compareTo(value) > 0)) {
                whereConditionSatified = false;
              }
              break;
            case "<=":
              if (!(columnValue.compareTo(value) <= 0)) {
                whereConditionSatified = false;
              }
              break;
            case ">=":
              if (!(columnValue.compareTo(value) >= 0)) {
                whereConditionSatified = false;
              }
              break;
            default:
              throw new RuntimeException("Invalid operator: " + operator);
          }
        }

        if (joinConditionSatisfied && whereConditionSatified) {
          for (LinkedList<Row> oldrow : oldrows) {
            LinkedList<Row> newRow = new LinkedList<>(oldrow);
            newRow.add(row);
            newrows.add(newRow);
          }
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
          System.out.println(
              metaInfo.getTableName()
                  + " "
                  + column.getName()
                  + column.getIndex()
                  + entries.toString());
          newEntries[i] = entries.get(column.getIndex());
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
