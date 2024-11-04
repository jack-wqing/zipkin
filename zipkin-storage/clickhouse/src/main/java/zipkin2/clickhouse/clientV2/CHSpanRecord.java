package zipkin2.clickhouse.clientV2;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

public class CHSpanRecord {
  private String traceId;
  private String parentId;
  private String id;
  private String kind;
  private String name;
  private long duration;
  private long timestamp;
  private long timestampMillis;
  private String time;
  private String serviceName;
  private Map<String, String> localEndpoint = Maps.newHashMap();
  private Map<String, String> remoteEndpoint = Maps.newHashMap();
  private Map<String, String> annotations = Maps.newHashMap();
  private Map<String, String> tags = Maps.newHashMap();

  public String getTraceId() {
    return traceId;
  }

  public void setTraceId(String traceId) {
    this.traceId = traceId;
  }

  public String getParentId() {
    return parentId;
  }

  public void setParentId(String parentId) {
    this.parentId = parentId;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getKind() {
    return kind;
  }

  public void setKind(String kind) {
    this.kind = kind;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public long getDuration() {
    return duration;
  }

  public void setDuration(long duration) {
    this.duration = duration;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public long getTimestampMillis() {
    return timestampMillis;
  }

  public void setTimestampMillis(long timestampMillis) {
    this.timestampMillis = timestampMillis;
  }

  public String getTime() {
    return time;
  }

  public void setTime(String time) {
    this.time = time;
  }

  public String getServiceName() {
    return serviceName;
  }

  public void setServiceName(String serviceName) {
    this.serviceName = serviceName;
  }

  public Map<String, String> getLocalEndpoint() {
    return localEndpoint;
  }

  public void setLocalEndpoint(Map<String, String> localEndpoint) {
    this.localEndpoint = localEndpoint;
  }

  public Map<String, String> getRemoteEndpoint() {
    return remoteEndpoint;
  }

  public void setRemoteEndpoint(Map<String, String> remoteEndpoint) {
    this.remoteEndpoint = remoteEndpoint;
  }

  public Map<String, String> getAnnotations() {
    return annotations;
  }

  public void setAnnotations(Map<String, String> annotations) {
    this.annotations = annotations;
  }

  public Map<String, String> getTags() {
    return tags;
  }

  public void setTags(Map<String, String> tags) {
    this.tags = tags;
  }

  public void addEndpoint(String key, String value, boolean local) {
    if (StringUtils.isBlank(key)) {
      return;
    }
    value = StringUtils.isBlank(value) ? "" : value;
    if (local) {
      localEndpoint.put(key, value);
    } else {
      remoteEndpoint.put(key, value);
    }
  }
  public void addAnnotation(String key, String value) {
    if (StringUtils.isBlank(key)) {
      return;
    }
    value = StringUtils.isBlank(value) ? "" : value;
    annotations.put(key, value);
  }

  public void addTag(String key, String value) {
    if (StringUtils.isBlank(key)) {
      return;
    }
    value = StringUtils.isBlank(value) ? "" : value;
    tags.put(key, value);
  }

}
