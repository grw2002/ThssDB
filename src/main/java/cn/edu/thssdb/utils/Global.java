package cn.edu.thssdb.utils;

public class Global {
  public static int fanout = 129;
  public static int MAX_PAGES = 10;

  public static int SUCCESS_CODE = 0;
  public static int FAILURE_CODE = -1;
  public static String DEFAULT_SERVER_HOST = "127.0.0.1";
  public static int DEFAULT_SERVER_PORT = 6667;
  public static String DEFAULT_USER_NAME = "root";
  public static String DEFAULT_PASSWORD = "root";
  public static String CLI_PREFIX = "ThssDB2023>";
  public static final String SHOW_TIME = "show time;";
  public static final String CONNECT = "connect";
  public static final String DISCONNECT = "disconnect;";
  public static final String QUIT = "quit;";
  public static final String S_URL_INTERNAL = "jdbc:default:connection";
  public static final boolean simpleMode = false;
  public static final String METADATA_FILE_NAME = "metadata.meta";
  public static final long ADMINISTER_SESSION = 114514;
  public static final boolean WAL_SWITCH = false;
  public static final boolean PERSIST_METADATA_SWITCH = true;
  public static final boolean PERSIST_DATA_SWITCH = true;
}
