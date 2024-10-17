package zipkin2.clickhouse;

import com.google.common.collect.Maps;
import javafx.beans.NamedArg;
import org.apache.commons.lang3.StringUtils;
import zipkin2.DependencyLink;
import zipkin2.Endpoint;
import zipkin2.Span;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ResultSetToSpanHelper {

  public static final List<Span> resultSetToSpan(ResultSet resultSet) throws SQLException {
    List<Span> spans = new ArrayList<>();
    while (resultSet.next()) {
      Span.Builder spanBuilder = Span.newBuilder();
      String traceId = resultSet.getString("traceId");
      String parentId = resultSet.getString("parentId");
      String id = resultSet.getString("id");
      String kind = resultSet.getString("kind");
      Long duration = resultSet.getLong("duration");
      String name = resultSet.getString("name");
      String timestamp = resultSet.getString("timestamp");
      String status = resultSet.getString("status");
      Long timestampMillis = resultSet.getLong("timestampMillis");
      String localEndpointIpv4 = resultSet.getString("localEndpointIpv4");
      String localEndpointServiceName = resultSet.getString("localEndpointServiceName");
      String localEndpointPort = resultSet.getString("localEndpointPort");
      String remoteEndpointIpv4 = resultSet.getString("remoteEndpointIpv4");
      String remoteEndpointServiceName = resultSet.getString("remoteEndpointServiceName");
      String remoteEndpointPort = resultSet.getString("remoteEndpointPort");
      String tagsRpcMethod = resultSet.getString("tagsRpcMethod");
      String tagsRpcService = resultSet.getString("tagsRpcService");
      String tagsAppname = resultSet.getString("tagsAppname");
      String tagsComponent = resultSet.getString("tagsComponent");
      String tagsHttpUrl = resultSet.getString("tagsHttpUrl");
      String tagsHttpMethod = resultSet.getString("tagsHttpMethod");
      if (timestampMillis != null) {
        spanBuilder.putTag("timestampMillis", timestampMillis.toString());
      }
      String tagsCatMessageId = resultSet.getString("tagsCatMessageId");
      String tagsHttpPath = resultSet.getString("tagsHttpPath");
      Integer tagsHttpRequestSize = resultSet.getInt("tagsHttpRequestSize");
      Integer tagsHttpResponseSize = resultSet.getInt("tagsHttpResponseSize");
      Integer tagsHttpStatusCode = resultSet.getInt("tagsHttpStatusCode");
      String tagsLocalIpv4 = resultSet.getString("tagsLocalIpv4");
      String tagsPeerIpv4 = resultSet.getString("tagsPeerIpv4");
      String tagsPeerService = resultSet.getString("tagsPeerService");
      Integer tagsPeerPort = resultSet.getInt("tagsPeerPort");
      String tagsPeerHostname = resultSet.getString("tagsPeerHostname");
      String tagsPeerIpv6 = resultSet.getString("tagsPeerIpv6");
      Integer tagsProcessId = resultSet.getInt("tagsProcessId");
      String tagsSpanKind = resultSet.getString("tagsSpanKind");
      Integer tagsWorkerId = resultSet.getInt("tagsWorkerId");
      String logFilePath = resultSet.getString("logFilePath");
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

  public static final List<DependencyLink> resultSetToSpanDependency(ResultSet resultSet) throws SQLException {
    List<DependencyLink> linkerList = new ArrayList<>();
    Map<Pair<String, String>, Long> linkerMap = Maps.newHashMap();
    while (resultSet.next()) {
      String kindStr = resultSet.getString("kind");
      String serviceName = resultSet.getString("localEndpointServiceName");
      String remoteServiceName = resultSet.getString("remoteEndpointServiceName");
      long count = resultSet.getLong("count");
      if (StringUtils.isBlank(serviceName) || StringUtils.isBlank(remoteServiceName)) {
        continue;
      }
      Span.Kind kind = Span.Kind.valueOf(kindStr);
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
      linkerMap.put(keyPair, sumCount + count);
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
        if (key != null ? !key.equals(pair.key) : pair.key != null) return false;
        if (value != null ? !value.equals(pair.value) : pair.value != null) return false;
        return true;
      }
      return false;
    }
  }

}
