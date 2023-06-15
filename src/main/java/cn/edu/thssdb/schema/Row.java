package cn.edu.thssdb.schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

public abstract class Row implements Serializable {
  protected static final long serialVersionUID = -5709782578272943998L;
  //  private int pageId;
  //  private int offset;

  public String toString() {
    List<Entry> entries = getEntries();
    if (entries == null) return "EMPTY";
    StringJoiner sj = new StringJoiner(", ");
    for (Entry e : entries) sj.add(e.toString());
    return sj.toString();
  }

  public abstract void addEntry(Entry entry);

  public abstract void dropEntry(int index);

  public abstract void alterEntryType(int index, Entry newEntry);

  public abstract void alterEntryName(int index, Entry newEntry);

  public abstract List<Entry> getEntries();

  public abstract void appendEntries(ArrayList<Entry> entries);
}
