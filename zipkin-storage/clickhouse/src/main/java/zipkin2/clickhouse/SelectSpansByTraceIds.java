package zipkin2.clickhouse;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.query.QueryResponse;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import zipkin2.Span;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class SelectSpansByTraceIds implements Function<Client, List<Span>> {
  private static final Logger logger = Logger.getLogger(SelectSpansByTraceIds.class.getName());
  private List<String> traceIds;
  private String traceTable;

  public SelectSpansByTraceIds(String traceTable, List<String> traceIds) {
    this.traceTable = traceTable;
    this.traceIds = traceIds;
  }
  @Override
  public List<Span> apply(Client client) {
    List<Span> result = Lists.newArrayList();
    String traceIdsStr = traceIds.stream().filter(StringUtils::isNotBlank).map(traceId -> "'" + traceId + "'")
      .collect(Collectors.joining(","));
    try (QueryResponse response = client.query(String.format(Constants.SPAN_TRACE_IDS_SQL, traceTable, traceIdsStr)).get(10, TimeUnit.SECONDS)) {
      ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);
      result = ResultSetToSpanHelper.resultSetToSpan(reader);
    } catch (ExecutionException e) {
      logger.log(Level.WARNING, "clickhouse SelectSpansByTraceIds ExecutionException", e);
    } catch (InterruptedException e) {
      logger.log(Level.WARNING, "clickhouse SelectSpansByTraceIds InterruptedException", e);
    } catch (TimeoutException e) {
      logger.log(Level.WARNING, "clickhouse SelectSpansByTraceIds TimeoutException", e);
    } catch (Exception e) {
      logger.log(Level.WARNING, "clickhouse SelectSpansByTraceIds Exception", e);
    }
    return result;
  }
}
