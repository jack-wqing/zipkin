package zipkin2.clickhouse;

import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.google.common.collect.Maps;
import javafx.beans.NamedArg;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import zipkin2.DependencyLink;
import zipkin2.Endpoint;
import zipkin2.Span;

import java.math.BigInteger;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ResultSetToSpanHelper {

  private static final Logger logger = Logger.getLogger(ResultSetToSpanHelper.class.getName());

  public static final List<Span> resultSetToSpan(ClickHouseBinaryFormatReader reader) throws SQLException {
    List<Span> spans = new ArrayList<>();
    while (reader.hasNext()) {
      Map<String, Object> next = reader.next();
      if (MapUtils.isEmpty(next)) {
        continue;
      }
      Span.Builder spanBuilder = Span.newBuilder();
      String traceId = findValue("traceId", () -> reader.getString("traceId"));
      if (StringUtils.isBlank(traceId)) {
        continue;
      }
      String parentId = findValue("parentId", () -> reader.getString("parentId"));
      String id = findValue("id", () -> reader.getString("id"));
      String kind = findValue("kind", () -> reader.getString("kind"));
      Long duration = findValue("duration", () -> reader.getLong("duration"));
      String name = findValue("name", () -> reader.getString("name"));
      String timestamp = findValue("timestamp", () -> reader.getString("timestamp"));
      String status = findValue("status", () -> reader.getString("status"));
      BigInteger timestampMillis = findValue("timestampMillis", () -> reader.getBigInteger("timestampMillis"));
      String localEndpointIpv4 = findValue("localEndpointIpv4", () -> reader.getString("localEndpointIpv4"));
      String localEndpointServiceName = findValue("localEndpointServiceName", () -> reader.getString("localEndpointServiceName"));
      String localEndpointPort = findValue("localEndpointPort", () -> reader.getString("localEndpointPort"));
      String remoteEndpointIpv4 = findValue("remoteEndpointIpv4", () -> reader.getString("remoteEndpointIpv4"));
      String remoteEndpointServiceName = findValue("remoteEndpointServiceName", () -> reader.getString("remoteEndpointServiceName"));
      String remoteEndpointPort = findValue("remoteEndpointPort", () -> reader.getString("remoteEndpointPort"));
      String tagsRpcMethod = findValue("tagsRpcMethod", () -> reader.getString("tagsRpcMethod"));
      String tagsRpcService = findValue("tagsRpcService", () -> reader.getString("tagsRpcService"));
      String tagsAppname = findValue("tagsAppname", () -> reader.getString("tagsAppname"));
      String tagsComponent = findValue("tagsComponent", () -> reader.getString("tagsComponent"));
      String tagsHttpUrl = findValue("tagsHttpUrl", () -> reader.getString("tagsHttpUrl"));
      String tagsHttpMethod = findValue("tagsHttpMethod", () -> reader.getString("tagsHttpMethod"));
      if (timestampMillis != null) {
        spanBuilder.putTag("timestampMillis", timestampMillis.toString());
      }
      String tagsCatMessageId = findValue("tagsCatMessageId", () -> reader.getString("tagsCatMessageId"));
      String tagsHttpPath = findValue("tagsHttpPath", () -> reader.getString("tagsHttpPath"));
      Long tagsHttpRequestSize = findValue("tagsHttpPath", () -> reader.getLong("tagsHttpRequestSize"));
      Long tagsHttpResponseSize = findValue("tagsHttpResponseSize", () -> reader.getLong("tagsHttpResponseSize"));
      Long tagsHttpStatusCode = findValue("tagsHttpStatusCode", () -> reader.getLong("tagsHttpStatusCode"));
      String tagsLocalIpv4 = findValue("tagsLocalIpv4", () -> reader.getString("tagsLocalIpv4"));
      String tagsPeerIpv4 = findValue("tagsPeerIpv4", () -> reader.getString("tagsPeerIpv4"));
      String tagsPeerService = findValue("tagsPeerService", () -> reader.getString("tagsPeerService"));
      Long tagsPeerPort = findValue("tagsPeerPort", () -> reader.getLong("tagsPeerPort"));
      String tagsPeerHostname = findValue("tagsPeerHostname", () -> reader.getString("tagsPeerHostname"));
      String tagsPeerIpv6 = findValue("tagsPeerIpv6", () -> reader.getString("tagsPeerIpv6"));
      Long tagsProcessId = findValue("tagsProcessId", () -> reader.getLong("tagsProcessId"));
      String tagsSpanKind = findValue("tagsSpanKind", () -> reader.getString("tagsSpanKind"));
      Long tagsWorkerId = findValue("tagsWorkerId", () -> reader.getLong("tagsWorkerId"));
      String logFilePath = findValue("logFilePath", () -> reader.getString("logFilePath"));
      spanBuilder.traceId(traceId);
      if (parentId != null && !parentId.equals("")) {
        spanBuilder.parentId(parentId);
      }
      if (id != null && !id.equals("")) {
        spanBuilder.id(id);
      }
      if (StringUtils.isNotBlank(kind)) {
        spanBuilder.kind(Span.Kind.valueOf(kind));
      }
      if (duration != null) {
        spanBuilder.duration(duration);
      }
      spanBuilder.name(name);
      if (timestamp != null && timestamp.contains(".")) {
        timestamp = timestamp.substring(0, timestamp.indexOf("."));
      }
      if (timestamp != null) {
        spanBuilder.timestamp(Long.parseLong(timestamp));
      }
      if (StringUtils.isNotBlank(status)) {
        spanBuilder.putTag("status", status);
      }
      Endpoint.Builder localEndpointBuilder = Endpoint.newBuilder();
      localEndpointBuilder.ip(localEndpointIpv4);
      localEndpointBuilder.serviceName(localEndpointServiceName);
      if (localEndpointPort != null && !localEndpointPort.equals("")) {
        localEndpointBuilder.port(Integer.parseInt(localEndpointPort));
      }
      Endpoint.Builder remoteEndpointBuilder = Endpoint.newBuilder();
      remoteEndpointBuilder.ip(remoteEndpointIpv4);
      remoteEndpointBuilder.serviceName(remoteEndpointServiceName);
      if (remoteEndpointPort != null && !remoteEndpointPort.equals("")) {
        remoteEndpointBuilder.port(Integer.parseInt(remoteEndpointPort));
      }
      spanBuilder.localEndpoint(localEndpointBuilder.build());
      spanBuilder.remoteEndpoint(remoteEndpointBuilder.build());
      if (StringUtils.isNotBlank(tagsRpcMethod)) {
        spanBuilder.putTag("rpc.method", tagsRpcMethod);
      }
      if (StringUtils.isNotBlank(tagsRpcService)) {
        spanBuilder.putTag("rpc.service", tagsRpcService);
      }
      if (StringUtils.isNotBlank(tagsAppname)) {
        spanBuilder.putTag("appname", tagsAppname);
      }
      if (StringUtils.isNotBlank(tagsComponent)) {
        spanBuilder.putTag("component", tagsComponent);
      }
      if (StringUtils.isNotBlank(tagsHttpUrl)) {
        spanBuilder.putTag("http.url", tagsHttpUrl);
      }
      if (StringUtils.isNotBlank(tagsHttpMethod)) {
        spanBuilder.putTag("http.method", tagsHttpMethod);
      }
      if (StringUtils.isNotBlank(tagsCatMessageId)) {
        spanBuilder.putTag("CatMessageId", tagsCatMessageId);
      }
      if (StringUtils.isNotBlank(tagsHttpPath)) {
        spanBuilder.putTag("http.path", tagsHttpPath);
      }
      if (tagsHttpRequestSize != null && tagsHttpRequestSize != 0) {
        spanBuilder.putTag("http.request.size", tagsHttpRequestSize.toString());
      }
      if (tagsHttpResponseSize != null && tagsHttpResponseSize != 0) {
        spanBuilder.putTag("http.response.size", tagsHttpResponseSize.toString());
      }
      if (tagsHttpStatusCode != null && tagsHttpStatusCode != 0) {
        spanBuilder.putTag("http.status.code", tagsHttpStatusCode.toString());
      }
      if (StringUtils.isNotBlank(tagsLocalIpv4)) {
        spanBuilder.putTag("local.ipv4", tagsLocalIpv4);
      }
      if (StringUtils.isNotBlank(tagsPeerIpv4)) {
        spanBuilder.putTag("peer.ipv4", tagsPeerIpv4);
      }
      if (StringUtils.isNotBlank(tagsPeerService)) {
        spanBuilder.putTag("peer.service", tagsPeerService);
      }
      if (tagsPeerPort != null && tagsPeerPort != 0) {
        spanBuilder.putTag("peer.port", tagsPeerPort.toString());
      }
      if (StringUtils.isNotBlank(tagsPeerHostname)) {
        spanBuilder.putTag("peer.hostname", tagsPeerHostname);
      }
      if (StringUtils.isNotBlank(tagsPeerIpv6)) {
        spanBuilder.putTag("peer.ipv6", tagsPeerIpv6);
      }
      if (tagsProcessId != null && tagsProcessId != 0) {
        spanBuilder.putTag("process.id", tagsProcessId.toString());
      }
      if(StringUtils.isNotBlank(tagsSpanKind)) {
        spanBuilder.putTag("span.kind", tagsSpanKind);
      }
      if (tagsWorkerId != null && tagsWorkerId != 0) {
        spanBuilder.putTag("worker.id", tagsWorkerId.toString());
      }
      if (StringUtils.isNotBlank(logFilePath)) {
        spanBuilder.putTag("log.file.path", logFilePath);
      }
      spans.add(spanBuilder.build());
    }
    return spans;
  }

  public static final List<DependencyLink> resultSetToSpanDependency(ClickHouseBinaryFormatReader reader) throws SQLException {
    List<DependencyLink> linkerList = new ArrayList<>();
    Map<Pair<String, String>, Long> linkerMap = Maps.newHashMap();
    while (reader.hasNext()) {
      reader.next();
      String kindStr = reader.getString("kind");
      String serviceName = reader.getString("localEndpointServiceName");
      String remoteServiceName = reader.getString("remoteEndpointServiceName");
      BigInteger count = reader.getBigInteger("count");
      serviceName = Objects.isNull(serviceName) ? "" : serviceName;
      remoteServiceName = Objects.isNull(remoteServiceName) ? "" : remoteServiceName;
      Span.Kind kind = StringUtils.isBlank(kindStr) ? null:  Span.Kind.valueOf(kindStr);
      if (kind == null) {
        // Treat unknown type of span as a client span if we know both sides
        if (serviceName != null && remoteServiceName != null) {
          kind = Span.Kind.CLIENT;
        } else {
          continue;
        }
      }
      String child;
      String parent;
      switch (kind) {
        case SERVER:
        case CONSUMER:
          child = serviceName;
          parent = remoteServiceName;
          break;
        case CLIENT:
        case PRODUCER:
          parent = serviceName;
          child = remoteServiceName;
          break;
        default:
          continue;
      }
      Pair<String, String> keyPair = new Pair<>(parent, child);
      Long sumCount = linkerMap.getOrDefault(keyPair, 0L);
      linkerMap.put(keyPair, sumCount + count.longValue());
    }
    linkerMap.forEach((k, v) -> {
      linkerList.add(DependencyLink.newBuilder().parent(k.getKey()).child(k.getValue()).callCount(v).build());
    });
    return linkerList;
  }

  static class Pair<K,V> {

    private K key;

    public K getKey() { return key; }

    private V value;

    public V getValue() { return value; }

    public Pair(@NamedArg("key") K key, @NamedArg("value") V value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public String toString() {
      return key + "=" + value;
    }

    @Override
    public int hashCode() {
      // name's hashCode is multiplied by an arbitrary prime number (13)
      // in order to make sure there is a difference in the hashCode between
      // these two parameters:
      //  name: a  value: aa
      //  name: aa value: a
      return key.hashCode() * 13 + (value == null ? 0 : value.hashCode());
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o instanceof Pair) {
        Pair pair = (Pair) o;
        if (!Objects.equals(key, pair.key)) return false;
        if (!Objects.equals(value, pair.value)) return false;
        return true;
      }
      return false;
    }
  }

  public static  <T> T findValue(String column, Supplier<T> supplier) {
    try {
      return supplier.get();
    } catch (Exception e) {
      logger.log(Level.WARNING, "findValue column:" + column + ",message" + e.getMessage());
    }
    return null;
  }

}
