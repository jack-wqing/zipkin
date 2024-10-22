package zipkin2.clickhouse;

/**
 * @Author:liuwenqing
 * @Date:2024/10/9 19:53
 * @Description:
 **/
public class Constants {

  public static final String BLANK_CHAR = " ";

  public static final String SINGLE_QUOTA = "'";

  public static final String BATCH_INSERT_THREAD_POOL_NAME = "ClickHouseInsertPool";

  public static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

  public static final String CHECK_SQL = "select traceId from %s limit 1";
  public static final String SERVICE_SQL = "select distinct localEndpointServiceName from %s where time >= '%s' AND time <= '%s'";

  public static final String SERVICE_TERM_SQL = "select distinct %s from %s where localEndpointServiceName = '%s' AND time >= '%s' AND time <= '%s'";

  public static final String SPAN_TRACE_ID_SQL = "select * from %s where traceId = '%s'";

  public static final String SPAN_TRACE_IDS_SQL = "select * from %s where traceId in (%s)";

  public static final String SPAN_QUERY_REQUEST_SQL = "select * from %s where  id != ''";

  public static final String TAG_VALUE_SQL = "select distinct %s from %s where time >= '%s' AND time <= '%s'";

  public static final String DEPENDENCY_SQL = "select kind, localEndpointServiceName, remoteEndpointServiceName, count(*) as count from %s "
    + "where time >= '%s' AND time <= '%s' ";
  public static final String DEPENDENCY_SQL_SUFFIX = "AND remoteEndpointServiceName NOT LIKE '%:%' AND localEndpointServiceName NOT LIKE '%:%' "
    + "group by kind,localEndpointServiceName,remoteEndpointServiceName";

}
