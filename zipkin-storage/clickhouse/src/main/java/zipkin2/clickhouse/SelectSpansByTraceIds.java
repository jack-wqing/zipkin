package zipkin2.clickhouse;

import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.ClickHouseStatement;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import zipkin2.Span;

import java.sql.ResultSet;
import java.util.List;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class SelectSpansByTraceIds implements Function<ClickHouseConnection, List<Span>> {
  private static final Logger LOG = Logger.getLogger(SelectSpansByTraceIds.class.getName());
  private List<String> traceIds;
  private String traceTable;

  public SelectSpansByTraceIds(String traceTable, List<String> traceIds) {
    this.traceTable = traceTable;
    this.traceIds = traceIds;
  }
  @Override
  public List<Span> apply(ClickHouseConnection connection) {
    List<Span> result = Lists.newArrayList();
    String traceIdsStr = traceIds.stream().filter(StringUtils::isNotBlank).map(traceId -> "'" + traceId + "'")
      .collect(Collectors.joining(","));
    try (ClickHouseStatement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(String.format(Constants.SPAN_TRACE_IDS_SQL, traceTable, traceIdsStr));
      result = ResultSetToSpanHelper.resultSetToSpan(resultSet);
    } catch (Exception e) {
      LOG.log(Level.WARNING, "select spans error: table:" + traceTable + " traceIds :" + traceIdsStr);
    }
    return result;
  }
}
