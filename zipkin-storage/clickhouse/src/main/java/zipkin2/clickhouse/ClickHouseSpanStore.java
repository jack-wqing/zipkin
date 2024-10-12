package zipkin2.clickhouse;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import zipkin2.Call;
import zipkin2.DependencyLink;
import zipkin2.Span;
import zipkin2.storage.GroupByTraceId;
import zipkin2.storage.QueryRequest;
import zipkin2.storage.ServiceAndSpanNames;
import zipkin2.storage.SpanStore;
import zipkin2.storage.StrictTraceId;
import zipkin2.storage.Traces;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class ClickHouseSpanStore implements SpanStore, Traces, ServiceAndSpanNames {

  final DataSourceCall.Factory dataSourceFactory;
  final boolean strictTraceId;
  final boolean searchEnabled;
  final Call.Mapper<List<Span>, List<List<Span>>> groupByTraceId;
  final DataSourceCall<List<String>> getServiceNamesCall;

  private String spanTable;
  private String traceTable;
  private long namesLookback;

  public ClickHouseSpanStore(ClickHouseStorage storage) {
    dataSourceFactory = storage.dataSourceCallFactory;
    strictTraceId = storage.strictTraceId;
    searchEnabled = storage.searchEnabled;
    groupByTraceId = GroupByTraceId.create(strictTraceId);
    getServiceNamesCall = dataSourceFactory.create(new SelectAnnotationServiceNames(storage.spanTable, storage.namesLookback));
    spanTable = storage.spanTable;
    traceTable = storage.traceTable;
    this.namesLookback = storage.namesLookback;
  }

  @Override
  public Call<List<List<Span>>> getTraces(QueryRequest request) {
    Call<List<List<Span>>> result =
      dataSourceFactory.create(new SelectSpanQueryRequest(spanTable, request)).map(groupByTraceId);
    return strictTraceId ? result.map(StrictTraceId.filterTraces(request)) : result;
  }

  @Override
  public Call<List<Span>> getTrace(String hexTraceId) {
    hexTraceId = Span.normalizeTraceId(hexTraceId);
    DataSourceCall<List<Span>> result =
      dataSourceFactory.create(new SelectSpansByTraceId(traceTable, hexTraceId));
    return strictTraceId ? result.map(StrictTraceId.filterSpans(hexTraceId)) : result;
  }

  @Override
  public Call<List<List<Span>>> getTraces(Iterable<String> traceIds) {
    Set<String> normalizedTraceIds = new LinkedHashSet<>();
    for (String traceId : traceIds) {
      String hexTraceId = Span.normalizeTraceId(traceId);
      normalizedTraceIds.add(hexTraceId);
    }
    Call<List<List<Span>>> result =
      dataSourceFactory.create(new SelectSpansByTraceIds(traceTable, Lists.newArrayList(normalizedTraceIds)))
        .map(groupByTraceId);
    return strictTraceId ? result.map(StrictTraceId.filterTraces(normalizedTraceIds)) : result;
  }

  @Override
  public Call<List<String>> getServiceNames() {
    if (!searchEnabled){
      return Call.emptyList();
    }
    return getServiceNamesCall.clone();
  }

  @Override
  public Call<List<String>> getRemoteServiceNames(String serviceName) {
    if (!searchEnabled || StringUtils.isBlank(serviceName)) {
      return Call.emptyList();
    }
    DataSourceCall<List<String>> result = dataSourceFactory.create(
      new SelectServiceSpanName(spanTable, namesLookback, serviceName, "remoteEndpointServiceName"));
    return result;
  }

  @Override
  public Call<List<String>> getSpanNames(String serviceName) {
    if (!searchEnabled || StringUtils.isBlank(serviceName)) {
      return Call.emptyList();
    }
    DataSourceCall<List<String>> result = dataSourceFactory.create(
      new SelectServiceSpanName(spanTable, namesLookback, serviceName, "name"));
    return result;
  }

  @Override
  public Call<List<DependencyLink>> getDependencies(long endTs, long lookback) {
    if (endTs <= 0) throw new IllegalArgumentException("endTs <= 0");
    if (lookback <= 0) throw new IllegalArgumentException("lookback <= 0");
    DataSourceCall<List<DependencyLink>> result =
      dataSourceFactory.create(new ClickHouseAggregateDependencies(strictTraceId, spanTable, endTs, lookback));
    return result;
  }
}
