package zipkin2.clickhouse;

/**
 * @Author:liuwenqing
 * @Date:2024/10/9 19:53
 * @Description:
 **/
public class Constants {

  public static final String BLANK_CHAR = " ";

  public static final String BLANK = "";

  public static final String SINGLE_QUOTA = "'";

  public static final String COMMA = ",";

  public static final String BATCH_INSERT_THREAD_POOL_NAME = "ClickHouseInsertPool";

  public static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

  public static final String CHECK_SQL = "select traceId from %s limit 1";
  public static final String SERVICE_SQL = "select distinct serviceName from %s where time >= '%s' AND time <= '%s'";

  public static final String REMOTE_SERVICE_SQL = "select distinct remoteEndpoint['serviceName'] as serviceName  from %s where serviceName = '%s' AND time >= '%s' AND time <= '%s'";

  public static final String SPAN_NAME_SQL = "select distinct name from %s where serviceName = '%s' AND time >= '%s' AND time <= '%s'";

  public static final String SPAN_TRACE_ID_SQL = "select * from %s where traceId = '%s' limit 5000";

  public static final String SPAN_TRACE_IDS_SQL = "select * from %s where traceId in (%s) limit 5000";

  public static final String SPAN_QUERY_REQUEST_SQL = "select * from %s where  id != ''";

  public static final String TAG_VALUE_SQL = "select distinct tags['%s'] as %s from %s where time >= '%s' AND time <= '%s'";

  public static final String DEPENDENCY_SQL = "select kind, localEndpoint['serviceName'] as localServiceName, remoteEndpoint['serviceName'] as remoteServiceName, tags['errorStatus'] as errorStatus, count(*) as count from %s "
    + "where time >= '%s' AND time <= '%s' ";
  public static final String DEPENDENCY_SQL_SUFFIX = "AND localEndpoint['serviceName'] NOT LIKE '%:%' AND remoteEndpoint['serviceName'] NOT LIKE '%:%' "
    + "group by kind,localServiceName, remoteServiceName, errorStatus";

  public static final String ENDPOINT_SERVICE_NAME = "serviceName";

  public static final String SERVICE_NAME = "serviceName";

  public static final String ENDPOINT_IPV4 = "ipv4";

  public static final String ENDPOINT_IPV6 = "ipv6";

  public static final String ENDPOINT_PORT = "port";

  public static final String HTTP_METHOD = "http.method";

  public static final String TAG_ERROR = "error";
  public static final String TAG_ERROR_STATUS = "errorStatus";
  public static final String TAG_ERROR_STATUS_OK = "0";
  public static final String TAG_ERROR_STATUS_ERROR = "1";

  public static final String KIND = "kind";
  public static final String LOCAL_SERVICE_NAME = "localServiceName";
  public static final String REMOTE_SERVICE_NAME = "remoteServiceName";
  public static final String COUNT = "count";
  public static final String TRADE_ID = "traceId";
  public static final String PARENT_ID = "parentId";
  public static final String ID = "id";
  public static final String NAME = "name";
  public static final String DURATION = "duration";
  public static final String TIMESTAMP = "timestamp";
  public static final String LOCAL_ENDPOINT = "localEndpoint";
  public static final String REMOTE_ENDPOINT = "remoteEndpoint";
  public static final String ANNOTATIONS = "annotations";
  public static final String TAGS = "tags";

}
