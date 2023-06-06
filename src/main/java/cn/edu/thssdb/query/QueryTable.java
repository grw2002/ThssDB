package cn.edu.thssdb.query;

import cn.edu.thssdb.index.BPlusTreeIterator;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.utils.Pair;

import java.util.Iterator;

@Deprecated
public class QueryTable implements Iterator<Row> {

  Table table;
  BPlusTreeIterator iter;

  public QueryTable(Table table) {
    // TODO
    this.table = table;
    table.loadTableDataFromFile();
    iter = table.index.iterator();
  }

  @Override
  public boolean hasNext() {
    return iter.hasNext();
  }

  @Override
  public Row next() {
    Pair pair = iter.next();
    return (Row) pair.right;
  }
}
