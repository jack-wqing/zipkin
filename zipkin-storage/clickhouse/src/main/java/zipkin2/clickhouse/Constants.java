package zipkin2.clickhouse;

/**
 * @Author:liuwenqing
 * @Date:2024/10/9 19:53
 * @Description:
 **/
public class Constants {

  public static final String CHECK_SQL = "select traceId from %s limit 1";
  public static final String SERVICE_SQL = "select localEndpointServiceName from %s where time >= ? AND time <= ?";

  public static final String SERVICE_TERM_SQL = "select %s from %s where localEndpointServiceName = ? AND time >= ? AND time <= ?";

  public static final String SPAN_TRACE_ID_SQL = "select * from %s where traceId = '%s'";

  public static final String SPAN_TRACE_IDS_SQL = "select * from %s where traceId in (%s)";

  public static final String SPAN_QUERY_REQUEST_SQL = "select * from %s where localEndpointServiceName = ? AND time >= ? AND time <= ?";


  public static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

}
