package zipkin2.clickhouse;

import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.ClickHouseStatement;
import com.google.common.collect.Lists;
import zipkin2.Span;

import java.sql.ResultSet;
import java.util.List;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SelectSpansByTraceId implements Function<ClickHouseConnection, List<Span>> {
  private static final Logger LOG = Logger.getLogger(SelectSpansByTraceId.class.getName());
  private String traceId;
  private String traceTable;

  public SelectSpansByTraceId(String traceTable, String traceId) {
    this.traceTable = traceTable;
    this.traceId = traceId;
  }
  @Override
  public List<Span> apply(ClickHouseConnection connection) {
    List<Span> result = Lists.newArrayList();
    try (ClickHouseStatement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(String.format(Constants.SPAN_TRACE_ID_SQL, traceTable, traceId));
      result = ResultSetToSpanHelper.resultSetToSpan(resultSet);
    } catch (Exception e) {
      LOG.log(Level.WARNING, "select spans error: table:" + traceTable + " traceId :" + traceId);
    }
    return result;
  }
}
