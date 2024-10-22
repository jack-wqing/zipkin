package zipkin2.clickhouse;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.query.QueryResponse;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import zipkin2.Span;
import zipkin2.storage.QueryRequest;

import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SelectSpanQueryRequest implements Function<Client, List<Span>> {

  public static final Logger logger = Logger.getLogger(SelectSpanQueryRequest.class.getName());
  private String spanTable;
  private QueryRequest queryRequest;

  public SelectSpanQueryRequest(String spanTable, QueryRequest queryRequest) {
    this.spanTable = spanTable;
    this.queryRequest = queryRequest;
  }

  @Override
  public List<Span> apply(Client client) {
    //优化查询:时间段必须
    long endTs = queryRequest.endTs();
    long lookback = queryRequest.lookback();
    if (endTs <= 0 || lookback <= 0 || (endTs - lookback) <= 0) {
      return Collections.emptyList();
    }
    String querySql = String.format(Constants.SPAN_QUERY_REQUEST_SQL, spanTable);
    //拼接时间
    querySql += Constants.BLANK_CHAR + " AND time >= " + Constants.SINGLE_QUOTA + DateFormatUtils.format(new Date(endTs - lookback), Constants.DATE_FORMAT) + Constants.SINGLE_QUOTA +
      " AND time <= " + Constants.SINGLE_QUOTA + DateFormatUtils.format(new Date(endTs), Constants.DATE_FORMAT) + Constants.SINGLE_QUOTA;
    String serviceName = queryRequest.serviceName();
    if (StringUtils.isNotBlank(serviceName)) {
      querySql +=  Constants.BLANK_CHAR + "AND localEndpointServiceName = " + Constants.SINGLE_QUOTA + serviceName + Constants.SINGLE_QUOTA;
    }
    querySql = querySql + condition();
    try (QueryResponse response = client.query(querySql).get(10, TimeUnit.SECONDS)) {
      ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);
      List<Span> result = ResultSetToSpanHelper.resultSetToSpan(reader);
      return result;
    } catch (ExecutionException e) {
      logger.log(Level.WARNING, "clickhouse SelectSpanQueryRequest ExecutionException", e);
    } catch (InterruptedException e) {
      logger.log(Level.WARNING, "clickhouse SelectSpanQueryRequest InterruptedException", e);
    } catch (TimeoutException e) {
      logger.log(Level.WARNING, "clickhouse SelectSpanQueryRequest TimeoutException", e);
    } catch (Exception e) {
      logger.log(Level.WARNING, "clickhouse SelectSpanQueryRequest Exception", e);
    }
    return Collections.emptyList();
  }

  private String condition() {
    StringBuilder sqlCondition = new StringBuilder();
    String remoteServiceName = queryRequest.remoteServiceName();
    if (StringUtils.isNotBlank(remoteServiceName)) {
      sqlCondition.append(" AND remoteEndpointServiceName = " + Constants.SINGLE_QUOTA + remoteServiceName +  Constants.SINGLE_QUOTA);
    }
    String spanName = queryRequest.spanName();
    if (StringUtils.isNotBlank(spanName)) {
      sqlCondition.append(" AND name = " +  Constants.SINGLE_QUOTA + spanName + Constants.SINGLE_QUOTA);
    }
    String annotationQueryString = annotationQueryString();
    if (StringUtils.isNotBlank(annotationQueryString)) {
      sqlCondition.append(Constants.BLANK_CHAR + " AND " + annotationQueryString);
    }
    Long minDuration = queryRequest.minDuration();
    Long maxDuration = queryRequest.maxDuration();
    if (minDuration != null && minDuration.longValue() != 0 && maxDuration != null && maxDuration.longValue() != 0) {
      sqlCondition.append(" AND duration >= " + minDuration);
      sqlCondition.append(" AND duration <= " + maxDuration);
    } else if (minDuration != null && minDuration.longValue() != 0) {
      sqlCondition.append(" AND duration >= " + minDuration);
    } else if (maxDuration != null && maxDuration.longValue() != 0) {
      sqlCondition.append(" AND duration <= " + maxDuration);
    }
    int limit = queryRequest.limit();
    if (limit < 1 || limit > 10000) {
      limit = 10000;
    }
    sqlCondition.append(" limit " + limit);
    return sqlCondition.toString();
  }
  public String annotationQueryString() {
    StringBuilder result = new StringBuilder();
    for (Iterator<Map.Entry<String, String>> i = queryRequest.annotationQuery().entrySet().iterator(); i.hasNext(); ) {
      Map.Entry<String, String> next = i.next();
      if (next.getValue().isEmpty()) {
        continue;
      }
      result.append(next.getKey());
      result.append('=').append(next.getValue());
      if (i.hasNext()) result.append(" and ");
    }
    return result.length() > 0 ? result.toString() : null;
  }

}
