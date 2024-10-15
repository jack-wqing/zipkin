package zipkin2.clickhouse;

/**
 * @Author:liuwenqing
 * @Date:2024/10/9 19:53
 * @Description:
 **/
public class Constants {
  public static final String BLANK = "";

  public static final String SPAN_TABLE = "{spanTable}";

  public static final String RIGHT_BRACKET = ")";

  public static final String LEFT_BRACKET = "(";

  public static final String COMMA = ",";

  public static final String SINGLE_QUOTA = "'";

  public static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

  public static final String CHECK_SQL = "select traceId from %s limit 1";
  public static final String SERVICE_SQL = "select distinct localEndpointServiceName from %s where time >= ? AND time <= ?";

  public static final String SERVICE_TERM_SQL = "select distinct %s from %s where localEndpointServiceName = ? AND time >= ? AND time <= ?";

  public static final String SPAN_TRACE_ID_SQL = "select * from %s where traceId = '%s'";

  public static final String SPAN_TRACE_IDS_SQL = "select * from %s where traceId in (%s)";

  public static final String SPAN_QUERY_REQUEST_TIME_SQL = "select * from %s where time >= ? AND time <= ?";
  public static final String SPAN_QUERY_REQUEST_SQL = "select * from %s where localEndpointServiceName = ? AND time >= ? AND time <= ?";

  public static final String TAG_VALUE_SQL = "select distinct %s from %s where time >= ? AND time <= ?";

  public static final String DEPENDENCY_SQL = "select kind, localEndpointServiceName, remoteEndpointServiceName, count(*) as count from " + SPAN_TABLE + " "
    + "where time >= ? AND time <= ? AND remoteEndpointServiceName NOT LIKE '%:%' AND localEndpointServiceName NOT LIKE '%:%' "
    + "group by kind,localEndpointServiceName,remoteEndpointServiceName";

  public static final String INSERT_SQL = "INSERT INTO %s (traceId,parentId,id,kind,duration,name,timestamp,status,timestampMillis,localEndpointIpv4,"
    + "localEndpointServiceName,localEndpointPort,remoteEndpointIpv4,remoteEndpointServiceName,remoteEndpointPort,tagsRpcMethod,tagsRpcService,tagsAppname,"
    + "tagsComponent,tagsHttpUrl,tagsHttpMethod,tagsCatMessageId,tagsHttpPath,tagsHttpRequestSize,tagsHttpResponseSize,tagsHttpStatusCode,tagsLocalIpv4,"
    + "tagsPeerIpv4,tagsPeerService,tagsPeerPort,tagsPeerHostname,tagsPeerIpv6,tagsProcessId,tagsSpanKind,tagsWorkerId,logFilePath,time,tagsSql) VALUES ";


}
