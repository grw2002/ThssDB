package cn.edu.thssdb.benchmark;

import cn.edu.thssdb.benchmark.executor.TransactionTestExecutor;
import cn.edu.thssdb.server.ThssDB;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(TransactionTest.class);

  private static TransactionTestExecutor transactionTestExecutor;

  @BeforeClass
  public static void setUp() throws Exception {
    ThssDB.main(null);
    Thread.sleep(1000);
    transactionTestExecutor = new TransactionTestExecutor();
    LOGGER.info("======================== Create database  ======================== ");
    transactionTestExecutor.createAndUseDB();
    LOGGER.info("======================== Prepare data  ======================== ");
    transactionTestExecutor.prepareData();
  }

  @AfterClass
  public static void tearDown() {
    transactionTestExecutor.close();
  }

  @Test
  public void testLostUpdate() throws Exception {
    LOGGER.info("======================== Lost update ========================");
    transactionTestExecutor.testLostUpdate();
  }

  @Test
  public void testDirtyRead() throws Exception {
    LOGGER.info("======================== Dirty read ========================");
    transactionTestExecutor.testDirtyRead();
  }
}
