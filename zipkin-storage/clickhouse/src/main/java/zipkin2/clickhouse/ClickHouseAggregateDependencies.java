package zipkin2.clickhouse;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.query.QueryResponse;
import org.apache.commons.lang3.time.DateFormatUtils;
import zipkin2.DependencyLink;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ClickHouseAggregateDependencies implements Function<Client, List<DependencyLink>> {

  public static final Logger logger = Logger.getLogger(ClickHouseAggregateDependencies.class.getName());

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
  public List<DependencyLink> apply(Client client) {
    String sql = String.format(Constants.DEPENDENCY_SQL, spanTable,
      DateFormatUtils.format(new Date(endTs - lookback), Constants.DATE_FORMAT),
      DateFormatUtils.format(new Date(endTs), Constants.DATE_FORMAT)) + Constants.DEPENDENCY_SQL_SUFFIX;
    try (QueryResponse response = client.query(sql).get(10, TimeUnit.SECONDS)) {
      ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);
      List<DependencyLink> linkerList = ResultSetToSpanHelper.resultSetToSpanDependency(reader);
      return linkerList;
    } catch (ExecutionException e) {
      logger.log(Level.WARNING, "clickhouse ClickHouseAggregateDependencies ExecutionException", e);
    } catch (InterruptedException e) {
      logger.log(Level.WARNING, "clickhouse ClickHouseAggregateDependencies InterruptedException", e);
    } catch (TimeoutException e) {
      logger.log(Level.WARNING, "clickhouse ClickHouseAggregateDependencies TimeoutException", e);
    } catch (Exception e) {
      logger.log(Level.WARNING, "clickhouse ClickHouseAggregateDependencies Exception", e);
    }
    return Collections.emptyList();
  }
}
