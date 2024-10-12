package zipkin2.clickhouse;

import com.clickhouse.jdbc.ClickHouseConnection;
import org.apache.commons.lang3.time.DateFormatUtils;
import zipkin2.Call;
import zipkin2.DependencyLink;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.function.Function;

public class ClickHouseAggregateDependencies implements Function<ClickHouseConnection, List<DependencyLink>> {

  final String spanTable;
  final long endTs;
  final long lookback;
  final boolean strictTraceId;

  public ClickHouseAggregateDependencies(boolean strictTraceId, String spanTable, long endTs, long lookback) {
    this.strictTraceId = strictTraceId;
    this.spanTable = spanTable;
    this.endTs = endTs;
    this.lookback = lookback;
  }
  @Override
  public List<DependencyLink> apply(ClickHouseConnection connection) {
    try (PreparedStatement statement = connection.prepareStatement(Constants.DEPENDENCY_SQL.replace(Constants.SPAN_TABLE, spanTable))) {
      statement.setObject(1, DateFormatUtils.format(new Date(endTs - lookback), Constants.DATE_FORMAT));
      statement.setObject(2, DateFormatUtils.format(new Date(endTs), Constants.DATE_FORMAT));
      ResultSet resultSet = statement.executeQuery();
      List<DependencyLink> linkerList = ResultSetToSpanHelper.resultSetToSpanDependency(resultSet);
      return linkerList;
    } catch (SQLException e) {
      Call.propagateIfFatal(e);
    }
    return Collections.emptyList();
  }
}
