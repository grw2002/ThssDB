package cn.edu.thssdb.storage;

import cn.edu.thssdb.utils.Global;

import java.util.*;

public class Storage {
  final HashMap<String, List<PageInterface>> tablePages;

  public Storage() {
    this.tablePages = new HashMap<>();
  }

  public synchronized void addPage(PageInterface page) {
    String identifier = page.getIdentifier();
    //    String identifier = "0";
    if (!tablePages.containsKey(identifier)) {
      tablePages.put(identifier, new ArrayList<>());
    }
    List<PageInterface> pages = tablePages.get(identifier);
    if (pages.size() >= Global.MAX_PAGES) {
      long minx = Long.MAX_VALUE;
      int target = 0;
      for (int i = 0; i < pages.size(); i++) {
        PageInterface oldpage = pages.get(i);
        long x = oldpage.getLastVisit();
        if (x < minx) {
          target = i;
          minx = x;
        }
      }
      PageInterface oldPage = pages.get(target);
      pages.remove(target);
      oldPage.release();
    }
    pages.add(page);
  }
}
