package cn.edu.thssdb.query;

import cn.edu.thssdb.exception.ColumnNotExistException;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.sql.SQLParser;

import java.util.ArrayList;
import java.util.List;

public class QueryResult2 {

  private QueryTable2 queryTable;
  List<String> resultColumnNames;

  public QueryResult2(QueryTable2 queryTable, List<String> resultColumnNames) {
    this.queryTable = queryTable;
    this.resultColumnNames = resultColumnNames;
  }

  public static QueryResult2 makeResult(
      QueryTable2 queryTable2, List<SQLParser.ResultColumnContext> resultColumns) {
    List<Column> columns = new ArrayList<>();
    List<String> resultColumnNames = new ArrayList<>();
    List<Integer> resultColumnIndex = new ArrayList<>();
    for (SQLParser.ResultColumnContext resultColumn : resultColumns) {
      if (resultColumn.getText().equals("*")) {
        columns.addAll(queryTable2.getColumns());
        for (Column column : queryTable2.getColumns()) {
          resultColumnNames.add(column.getTableName() + '.' + column.getName());
        }
      } else if (resultColumn.tableName() != null) {
        String tableName = resultColumn.tableName().getText();
        for (Column column : queryTable2.getColumns()) {
          if (column.getTableName().equals(tableName)) {
            columns.add(column);
            resultColumnNames.add(column.getTableName() + '.' + column.getName());
          }
        }
      } else {
        SQLParser.ColumnFullNameContext columnFullName = resultColumn.columnFullName();
        Column column = null;
        if (columnFullName.tableName() == null) {
          column = queryTable2.findColumnByName(columnFullName.columnName().getText());
          // TODO : Test Duplicate AttrName
        } else {
          column =
              queryTable2.findColumnByName(
                  columnFullName.columnName().getText(), columnFullName.tableName().getText());
        }
        if (column == null) {
          throw new ColumnNotExistException(columnFullName.getText());
        }
        columns.add(column);
        resultColumnNames.add(columnFullName.getText());
        resultColumnIndex.add(column.getIndex());
      }
    }
    QueryTable2 resultTable = new QueryTable2("result", columns.toArray(new Column[0]));
    for (Row row : queryTable2) {
      Row newRow = new Row();
      for (Integer columnIndex : resultColumnIndex) {
        newRow.addEntry(row.getEntries().get(columnIndex));
      }
      resultTable.insert(newRow);
    }
    return new QueryResult2(resultTable, resultColumnNames);
  }

  public static List<String> rowToStringList(Row row) {
    List<String> result = new ArrayList<>();
    for (Entry entry : row.getEntries()) {
      result.add(entry.toString());
    }
    return result;
  }

  public List<String> getResultColumnNames() {
    return resultColumnNames;
  }

  public List<List<String>> getRowsList() {
    List<List<String>> result = new ArrayList<>();
    for (Row row : queryTable) {
      result.add(rowToStringList(row));
    }
    return result;
  }
}
