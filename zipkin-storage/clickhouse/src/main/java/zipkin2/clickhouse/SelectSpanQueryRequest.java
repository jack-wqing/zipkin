package zipkin2.clickhouse;

import com.clickhouse.jdbc.ClickHouseConnection;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import zipkin2.Span;
import zipkin2.storage.QueryRequest;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SelectSpanQueryRequest implements Function<ClickHouseConnection, List<Span>> {

  public static final Logger log = Logger.getLogger(SelectSpanQueryRequest.class.getName());

  private String spanTable;
  private QueryRequest queryRequest;

  public SelectSpanQueryRequest(String spanTable, QueryRequest queryRequest) {
    this.spanTable = spanTable;
    this.queryRequest = queryRequest;
  }

  @Override
  public List<Span> apply(ClickHouseConnection connection) {
    //优化查询:时间段必须
    long endTs = queryRequest.endTs();
    long lookback = queryRequest.lookback();
    if (endTs <= 0 || lookback <= 0 || (endTs - lookback) <= 0) {
      return Collections.emptyList();
    }
    //优化查询服务名必须
    String serviceName = queryRequest.serviceName();
    if(StringUtils.isBlank(serviceName)) {
      return Collections.emptyList();
    }
    String querySql = String.format(Constants.SPAN_QUERY_REQUEST_SQL, spanTable);
    StringBuilder sqlCondition = new StringBuilder();
    Map<Integer, Object> parmMap = condition(sqlCondition);
    querySql = querySql + sqlCondition;
    try(PreparedStatement statement = connection.prepareStatement(querySql)) {
      statement.setObject(1, serviceName);
      statement.setObject(2, DateFormatUtils.format(new Date(endTs - lookback), Constants.DATE_FORMAT));
      statement.setObject(3, DateFormatUtils.format(new Date(endTs), Constants.DATE_FORMAT));
      parmMap.forEach((k, v) -> {
          try {
              statement.setObject(k, v);
          } catch (SQLException e) {
            log.log(Level.WARNING, " statement.setObject", e);
          }
      });
      ResultSet resultSet = statement.executeQuery();
      List<Span> spans = ResultSetToSpanHelper.resultSetToSpan(resultSet);
      return spans;
    } catch (SQLException e) {
      log.log(Level.WARNING, "span search error", e);
    }
    return Collections.emptyList();
  }

  private Map<Integer, Object> condition(StringBuilder sqlCondition) {
    Map<Integer, Object> paramMap = Maps.newHashMap();
    String remoteServiceName = queryRequest.remoteServiceName();
    int index = 4;
    if (StringUtils.isNotBlank(remoteServiceName)) {
      sqlCondition.append(" AND remoteEndpointServiceName = ?");
      paramMap.put(index++, remoteServiceName);
    }
    String spanName = queryRequest.spanName();
    if (StringUtils.isNotBlank(spanName)) {
      sqlCondition.append(" AND name = ?");
      paramMap.put(index++, spanName);
    }
    String annotationQueryString = queryRequest.annotationQueryString();
    if (StringUtils.isNotBlank(spanName)) {
      sqlCondition.append(" " + annotationQueryString);
    }
    Long minDuration = queryRequest.minDuration();
    Long maxDuration = queryRequest.maxDuration();
    if (minDuration != null && minDuration.longValue() != 0 && maxDuration != null && maxDuration.longValue() != 0) {
      sqlCondition.append(" AND duration >= ?");
      paramMap.put(index++, minDuration);
      sqlCondition.append(" AND duration <= ?");
      paramMap.put(index++, maxDuration);
    } else if (minDuration != null && minDuration.longValue() != 0) {
      sqlCondition.append(" AND duration >= ?");
      paramMap.put(index++, minDuration);
    } else if (maxDuration != null && maxDuration.longValue() != 0) {
      sqlCondition.append(" AND duration <= ?");
      paramMap.put(index++, maxDuration);
    }
    int limit = queryRequest.limit();
    if (limit == 1) {
      limit = 10000;
    }
    sqlCondition.append(" limit ?");
    paramMap.put(index, limit);
    return paramMap;
  }

}
