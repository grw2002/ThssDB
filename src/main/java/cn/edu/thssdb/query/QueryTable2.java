package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.sql.SQLParser;
import cn.edu.thssdb.type.ColumnType;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.*;

public class QueryTable2 extends MetaInfo2 implements Iterable<Row>, Serializable {

  protected String queryName;
  // In memory
  private List<Row> rows;

  public String getQueryName() {
    return queryName;
  }

  public QueryTable2(String queryName, Column[] columns) {
    super(columns);
    for (Column column : columns) {
      column.setQueryTable(this);
    }
    this.queryName = queryName;
    this.columnIndex = new HashMap<>(); // 初始化映射
    this.rows = new ArrayList<>();
    updateColumnIndex();
  }

  public final void insert(Row[] rows) {
    for (Row row : rows) {
      insert(row);
    }
  }

  public void insert(Row row) {
    rows.add(row);
  }

  private static Comparable evaluateExpression(
      MetaInfo2 metaInfo, Row row, SQLParser.ExpressionContext expression) throws RuntimeException {
    SQLParser.ComparerContext comparer = expression.comparer();
    List<SQLParser.ExpressionContext> exps = expression.expression();
    if (comparer != null) {
      SQLParser.LiteralValueContext literalValue = comparer.literalValue();
      if (literalValue != null) {
        if (literalValue.NUMERIC_LITERAL() != null) {
          return new BigDecimal(literalValue.getText());
          //          return Double.parseDouble(literalValue.getText());
        } else if (literalValue.STRING_LITERAL() != null) {
          return literalValue.getText();
        } else if (literalValue.K_NULL() != null) {
          return null;
        } else {
          throw new RuntimeException("Invalid LiteralValue: " + literalValue.getText());
        }
      } else {
        SQLParser.ColumnFullNameContext columnFullName = comparer.columnFullName();
        if (columnFullName != null) {
          SQLParser.TableNameContext tableNameContext = columnFullName.tableName();
          String columnName = columnFullName.columnName().getText();
          //          int columnIndex;
          Column column;
          if (tableNameContext != null) {
            String tableName = tableNameContext.getText();
            column = metaInfo.findColumnByName(columnName, tableName);
          } else {
            column = metaInfo.findColumnByName(columnName);
          }
          Comparable value = row.getEntries().get(column.getIndex()).value;
          if (column.getType() == ColumnType.STRING) {
            return value.toString();
          } else {
            return new BigDecimal(value.toString());
          }
        } else {
          throw new RuntimeException("Invalid Comparer: " + comparer.getText());
        }
      }
    } else if (exps.size() == 1) {
      return evaluateExpression(metaInfo, row, exps.get(0));
    } else if (exps.size() == 2) {
      BigDecimal v1 = (BigDecimal) evaluateExpression(metaInfo, row, exps.get(0));
      BigDecimal v2 = (BigDecimal) evaluateExpression(metaInfo, row, exps.get(1));
      if (expression.MUL() != null) {
        return v1.multiply(v2);
      } else if (expression.DIV() != null) {
        return v1.divide(v2);
      } else if (expression.ADD() != null) {
        return v1.add(v2);
      } else if (expression.SUB() != null) {
        return v1.subtract(v2);
      } else {
        throw new RuntimeException("Invalid operator: " + expression.getText());
      }
    }
    throw new RuntimeException("Invalid Expression" + expression.getText());
  }

  private static boolean isConditionSatisfied(
      MetaInfo2 metaInfo, Row row, SQLParser.ConditionContext condition) throws RuntimeException {
    if (condition == null) {
      return true;
    }
    SQLParser.ExpressionContext exp1 = condition.expression(0);
    SQLParser.ExpressionContext exp2 = condition.expression(1);
    SQLParser.ComparatorContext op = condition.comparator();
    Comparable value1 = evaluateExpression(metaInfo, row, exp1);
    Comparable value2 = evaluateExpression(metaInfo, row, exp2);
    if (op.EQ() != null) {
      return value1.compareTo(value2) == 0;
    } else if (op.NE() != null) {
      return value1.compareTo(value2) != 0;
    } else if (op.GT() != null) {
      return value1.compareTo(value2) > 0;
    } else if (op.LT() != null) {
      return value1.compareTo(value2) < 0;
    } else if (op.GE() != null) {
      return value1.compareTo(value2) >= 0;
    } else if (op.LE() != null) {
      return value1.compareTo(value2) <= 0;
    } else {
      throw new RuntimeException("Invalid operator: " + op.getText());
    }
  }

  private static boolean isConditionSatisfied(
      MetaInfo2 metaInfo, Row row, SQLParser.MultipleConditionContext conditions)
      throws RuntimeException {
    if (conditions == null) {
      return true;
    }
    if (conditions.AND() != null) {
      return isConditionSatisfied(metaInfo, row, conditions.multipleCondition(0))
          && isConditionSatisfied(metaInfo, row, conditions.multipleCondition(1));
    } else if (conditions.OR() != null) {
      return isConditionSatisfied(metaInfo, row, conditions.multipleCondition(0))
          || isConditionSatisfied(metaInfo, row, conditions.multipleCondition(1));
    } else if (conditions.condition() != null) {
      return isConditionSatisfied(metaInfo, row, conditions.condition());
    }
    throw new RuntimeException("Invalid condition: " + conditions.getText());
  }

  public static QueryTable2 filterCondition(
      QueryTable2 queryTable, SQLParser.MultipleConditionContext conditions) {
    Iterator iter = queryTable.iterator();
    QueryTable2 newQueryTable =
        new QueryTable2(queryTable.getQueryName(), queryTable.getColumns().toArray(new Column[0]));
    while (iter.hasNext()) {
      Row row = (Row) iter.next();
      if (isConditionSatisfied(queryTable, row, conditions)) {
        newQueryTable.insert(row);
      }
    }
    return newQueryTable;
  }

  public static QueryTable2 filterCondition(
      QueryTable2 queryTable, SQLParser.ConditionContext condition) {
    Iterator iter = queryTable.iterator();
    QueryTable2 newQueryTable =
        new QueryTable2(queryTable.getQueryName(), queryTable.getColumns().toArray(new Column[0]));
    while (iter.hasNext()) {
      Row row = (Row) iter.next();
      if (isConditionSatisfied(queryTable, row, condition)) {
        newQueryTable.insert(row);
      }
    }
    return newQueryTable;
  }

  public static QueryTable2 joinQueryTables(
      List<QueryTable2> querytables, SQLParser.MultipleConditionContext conditions) {
    QueryTable2 newQueryTable = joinQueryTables(querytables);
    return filterCondition(newQueryTable, conditions);
  }

  public static QueryTable2 joinQueryTables(List<QueryTable2> querytables) {
    List<Column> columns = new ArrayList<>();
    List<String> tableNames = new ArrayList<>();
    List<Row> oldrows = new LinkedList<>();
    List<Row> newrows = new LinkedList<>();
    List<Row> tmp;
    oldrows.add(new Row());
    for (QueryTable2 querytable : querytables) {
      for (Column column : querytable.getColumns()) {
        columns.add(column.clone());
      }
      tableNames.add(querytable.getQueryName());
      for (Row currentRow : querytable) {
        for (Row oldrow : oldrows) {
          Row newrow = new Row();
          newrow.getEntries().addAll(oldrow.getEntries());
          newrow.getEntries().addAll(currentRow.getEntries());
          newrows.add(newrow);
        }
      }
      tmp = oldrows;
      oldrows = newrows;
      newrows = tmp;
      newrows.clear();
    }
    QueryTable2 newQueryTable =
        new QueryTable2(String.join(" joins ", tableNames), columns.toArray(new Column[0]));
    newQueryTable.insert(oldrows.toArray(new Row[0]));
    return newQueryTable;
  }

  // util: update column-index map

  @Override
  public void addColumn(Column column) {
    super.addColumn(column);
    column.setQueryTable(this);
    Iterator iter = this.iterator();
    while (iter.hasNext()) {
      Row row = (Row) iter.next();
      row.getEntries().add(new Entry(null));
    }
  }

  @Override
  public Iterator<Row> iterator() {
    return rows.iterator();
  }
}