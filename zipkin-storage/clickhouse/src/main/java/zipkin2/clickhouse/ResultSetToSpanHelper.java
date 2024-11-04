package zipkin2.clickhouse;

import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.google.common.collect.Maps;
import javafx.beans.NamedArg;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import zipkin2.DependencyLink;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.clickhouse.util.ConvertUtils;

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

  public static final List<Span> resultSetToSpan(ClickHouseBinaryFormatReader reader) {
    List<Span> spans = new ArrayList<>();
    while (reader.hasNext()) {
      Map<String, Object> next = reader.next();
      if (MapUtils.isEmpty(next)) {
        continue;
      }
      Span.Builder spanBuilder = Span.newBuilder();
      String traceId = findValue(Constants.TRADE_ID, () -> reader.getString(Constants.TRADE_ID));
      if (StringUtils.isBlank(traceId)) {
        continue;
      }
      String parentId = findValue(Constants.PARENT_ID, () -> reader.getString(Constants.PARENT_ID));
      String id = findValue(Constants.ID, () -> reader.getString(Constants.ID));
      String kind = findValue(Constants.KIND, () -> reader.getString(Constants.KIND));
      String name = findValue(Constants.NAME, () -> reader.getString(Constants.NAME));
      Long duration = findValue(Constants.DURATION, () -> reader.getLong(Constants.DURATION));
      BigInteger timestamp = findValue(Constants.TIMESTAMP, () -> reader.getBigInteger(Constants.TIMESTAMP));
      Map<String, String> localEndpoint = findValue(Constants.LOCAL_ENDPOINT, () -> reader.readValue(Constants.LOCAL_ENDPOINT));
      Map<String, String> remoteEndpoint = findValue(Constants.REMOTE_ENDPOINT, () -> reader.readValue(Constants.REMOTE_ENDPOINT));
      Map<String, String> annotations = findValue(Constants.ANNOTATIONS, () -> reader.readValue(Constants.ANNOTATIONS));
      Map<String, String> tags = findValue(Constants.TAGS, () -> reader.readValue(Constants.TAGS));

      spanBuilder.traceId(traceId);
      if(StringUtils.isNotBlank(parentId)) {
        spanBuilder.parentId(parentId);
      }
      if(StringUtils.isNotBlank(id)) {
        spanBuilder.id(id);
      }
      if (StringUtils.isNotBlank(kind)) {
        spanBuilder.kind(Span.Kind.valueOf(kind));
      }
      if (StringUtils.isNotBlank(name)) {
        spanBuilder.name(name);
      }
      if (duration != null) {
        spanBuilder.duration(duration);
      }
      if (timestamp != null) {
        spanBuilder.timestamp(timestamp.longValue());
      }
      spanBuilder.localEndpoint(endpoint(localEndpoint));
      spanBuilder.remoteEndpoint(endpoint(remoteEndpoint));
      annotations(spanBuilder, annotations);
      tags(spanBuilder, tags);
      spans.add(spanBuilder.build());
    }
    return spans;
  }

  private static Endpoint endpoint(Map<String, String> endpointMap) {
    Endpoint.Builder endpointBuilder = Endpoint.newBuilder();
    if (MapUtils.isEmpty(endpointMap)) {
      return endpointBuilder.build();
    }
    endpointBuilder.serviceName(endpointMap.get(Constants.ENDPOINT_SERVICE_NAME));
    endpointBuilder.ip(endpointMap.get(Constants.ENDPOINT_IPV4));
    endpointBuilder.ip(endpointMap.get(Constants.ENDPOINT_IPV6));
    String port = endpointMap.get(Constants.ENDPOINT_PORT);
    endpointBuilder.port(ConvertUtils.strToInteger(port));
    return endpointBuilder.build();
  }

  private static void annotations(Span.Builder spanBuilder, Map<String, String> annotationsMap ) {
    if (MapUtils.isEmpty(annotationsMap)) {
      return;
    }
    for (Map.Entry<String, String> entry : annotationsMap.entrySet()) {
      String timestampValue = entry.getValue();
      if (StringUtils.isBlank(timestampValue)) {
        continue;
      }
      spanBuilder.addAnnotation(ConvertUtils.strToLong(timestampValue), entry.getKey());
    }
  }

  private static void tags(Span.Builder spanBuilder, Map<String, String> tagsMap) {
    if (MapUtils.isEmpty(tagsMap)) {
      return;
    }
    for (Map.Entry<String, String> entry : tagsMap.entrySet()) {
      spanBuilder.putTag(entry.getKey(), entry.getValue());
    }
  }

  public static final List<DependencyLink> resultSetToSpanDependency(ClickHouseBinaryFormatReader reader) throws SQLException {
    List<DependencyLink> linkerList = new ArrayList<>();
    Map<Pair<String, String>, Pair<Long, Long>> linkerMap = Maps.newHashMap();
    while (reader.hasNext()) {
      reader.next();
      String kindStr = reader.getString(Constants.KIND);
      String serviceName = reader.getString(Constants.LOCAL_SERVICE_NAME);
      String remoteServiceName = reader.getString(Constants.REMOTE_SERVICE_NAME);
      String errorStatus = reader.getString(Constants.TAG_ERROR_STATUS);
      BigInteger count = reader.getBigInteger(Constants.COUNT);
      serviceName = Objects.isNull(serviceName) ? Constants.BLANK : serviceName;
      remoteServiceName = Objects.isNull(remoteServiceName) ? Constants.BLANK : remoteServiceName;
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
      Pair<Long, Long> countPair = linkerMap.getOrDefault(keyPair, new Pair<>(0L, 0L));
      if (StringUtils.equals(errorStatus, Constants.TAG_ERROR_STATUS_ERROR)) {
        countPair.setValue(countPair.getValue() + count.longValue());
      } else {
        countPair.setKey(countPair.getKey() + count.longValue());
      }
      linkerMap.put(keyPair, countPair);
    }
    linkerMap.forEach((k, v) -> linkerList.add(DependencyLink.newBuilder().parent(k.getKey()).child(k.getValue())
      .callCount(v.getKey() + v.getValue()).errorCount(v.getValue()).build()));
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

    public void setKey(K key) {
      this.key = key;
    }

    public void setValue(V value) {
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
