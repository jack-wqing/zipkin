package zipkin2.clickhouse.clientV2;


public class CHSpanRecord {
  private String traceId;
  private String parentId;
  private String id;
  private String kind;
  private long duration;
  private String name;
  private String timestamp;
  private String status;
  private long timestampMillis;
  private String localEndpointIpv4;
  private String localEndpointServiceName;
  private String localEndpointPort;
  private String remoteEndpointIpv4;
  private String remoteEndpointServiceName;
  private String remoteEndpointPort;
  private String tagsRpcMethod;
  private String tagsRpcService;
  private String tagsAppname;
  private String tagsComponent;
  private String tagsHttpUrl;
  private String tagsHttpMethod;
  private String tagsCatMessageId;
  private String tagsHttpPath;
  private long tagsHttpRequestSize;
  private long tagsHttpResponseSize;
  private long tagsHttpStatusCode;
  private String tagsLocalIpv4;
  private String tagsPeerIpv4;
  private String tagsPeerService;
  private long tagsPeerPort;
  private String tagsPeerHostname;
  private String tagsPeerIpv6;
  private long tagsProcessId;
  private String tagsSpanKind;
  private long tagsWorkerId;
  private String logFilePath;
  private String time;
  private String tagsSql;

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

  public long getDuration() {
    return duration;
  }

  public void setDuration(long duration) {
    this.duration = duration;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(String timestamp) {
    this.timestamp = timestamp;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public long getTimestampMillis() {
    return timestampMillis;
  }

  public void setTimestampMillis(long timestampMillis) {
    this.timestampMillis = timestampMillis;
  }

  public String getLocalEndpointIpv4() {
    return localEndpointIpv4;
  }

  public void setLocalEndpointIpv4(String localEndpointIpv4) {
    this.localEndpointIpv4 = localEndpointIpv4;
  }

  public String getLocalEndpointServiceName() {
    return localEndpointServiceName;
  }

  public void setLocalEndpointServiceName(String localEndpointServiceName) {
    this.localEndpointServiceName = localEndpointServiceName;
  }

  public String getLocalEndpointPort() {
    return localEndpointPort;
  }

  public void setLocalEndpointPort(String localEndpointPort) {
    this.localEndpointPort = localEndpointPort;
  }

  public String getRemoteEndpointIpv4() {
    return remoteEndpointIpv4;
  }

  public void setRemoteEndpointIpv4(String remoteEndpointIpv4) {
    this.remoteEndpointIpv4 = remoteEndpointIpv4;
  }

  public String getRemoteEndpointServiceName() {
    return remoteEndpointServiceName;
  }

  public void setRemoteEndpointServiceName(String remoteEndpointServiceName) {
    this.remoteEndpointServiceName = remoteEndpointServiceName;
  }

  public String getRemoteEndpointPort() {
    return remoteEndpointPort;
  }

  public void setRemoteEndpointPort(String remoteEndpointPort) {
    this.remoteEndpointPort = remoteEndpointPort;
  }

  public String getTagsRpcMethod() {
    return tagsRpcMethod;
  }

  public void setTagsRpcMethod(String tagsRpcMethod) {
    this.tagsRpcMethod = tagsRpcMethod;
  }

  public String getTagsRpcService() {
    return tagsRpcService;
  }

  public void setTagsRpcService(String tagsRpcService) {
    this.tagsRpcService = tagsRpcService;
  }

  public String getTagsAppname() {
    return tagsAppname;
  }

  public void setTagsAppname(String tagsAppname) {
    this.tagsAppname = tagsAppname;
  }

  public String getTagsComponent() {
    return tagsComponent;
  }

  public void setTagsComponent(String tagsComponent) {
    this.tagsComponent = tagsComponent;
  }

  public String getTagsHttpUrl() {
    return tagsHttpUrl;
  }

  public void setTagsHttpUrl(String tagsHttpUrl) {
    this.tagsHttpUrl = tagsHttpUrl;
  }

  public String getTagsHttpMethod() {
    return tagsHttpMethod;
  }

  public void setTagsHttpMethod(String tagsHttpMethod) {
    this.tagsHttpMethod = tagsHttpMethod;
  }

  public String getTagsCatMessageId() {
    return tagsCatMessageId;
  }

  public void setTagsCatMessageId(String tagsCatMessageId) {
    this.tagsCatMessageId = tagsCatMessageId;
  }

  public String getTagsHttpPath() {
    return tagsHttpPath;
  }

  public void setTagsHttpPath(String tagsHttpPath) {
    this.tagsHttpPath = tagsHttpPath;
  }

  public long getTagsHttpRequestSize() {
    return tagsHttpRequestSize;
  }

  public void setTagsHttpRequestSize(long tagsHttpRequestSize) {
    this.tagsHttpRequestSize = tagsHttpRequestSize;
  }

  public long getTagsHttpResponseSize() {
    return tagsHttpResponseSize;
  }

  public void setTagsHttpResponseSize(long tagsHttpResponseSize) {
    this.tagsHttpResponseSize = tagsHttpResponseSize;
  }

  public long getTagsHttpStatusCode() {
    return tagsHttpStatusCode;
  }

  public void setTagsHttpStatusCode(long tagsHttpStatusCode) {
    this.tagsHttpStatusCode = tagsHttpStatusCode;
  }

  public String getTagsLocalIpv4() {
    return tagsLocalIpv4;
  }

  public void setTagsLocalIpv4(String tagsLocalIpv4) {
    this.tagsLocalIpv4 = tagsLocalIpv4;
  }

  public String getTagsPeerIpv4() {
    return tagsPeerIpv4;
  }

  public void setTagsPeerIpv4(String tagsPeerIpv4) {
    this.tagsPeerIpv4 = tagsPeerIpv4;
  }

  public String getTagsPeerService() {
    return tagsPeerService;
  }

  public void setTagsPeerService(String tagsPeerService) {
    this.tagsPeerService = tagsPeerService;
  }

  public long getTagsPeerPort() {
    return tagsPeerPort;
  }

  public void setTagsPeerPort(long tagsPeerPort) {
    this.tagsPeerPort = tagsPeerPort;
  }

  public String getTagsPeerHostname() {
    return tagsPeerHostname;
  }

  public void setTagsPeerHostname(String tagsPeerHostname) {
    this.tagsPeerHostname = tagsPeerHostname;
  }

  public String getTagsPeerIpv6() {
    return tagsPeerIpv6;
  }

  public void setTagsPeerIpv6(String tagsPeerIpv6) {
    this.tagsPeerIpv6 = tagsPeerIpv6;
  }

  public long getTagsProcessId() {
    return tagsProcessId;
  }

  public void setTagsProcessId(long tagsProcessId) {
    this.tagsProcessId = tagsProcessId;
  }

  public String getTagsSpanKind() {
    return tagsSpanKind;
  }

  public void setTagsSpanKind(String tagsSpanKind) {
    this.tagsSpanKind = tagsSpanKind;
  }

  public long getTagsWorkerId() {
    return tagsWorkerId;
  }

  public void setTagsWorkerId(long tagsWorkerId) {
    this.tagsWorkerId = tagsWorkerId;
  }

  public String getLogFilePath() {
    return logFilePath;
  }

  public void setLogFilePath(String logFilePath) {
    this.logFilePath = logFilePath;
  }

  public String getTime() {
    return time;
  }

  public void setTime(String time) {
    this.time = time;
  }

  public String getTagsSql() {
    return tagsSql;
  }

  public void setTagsSql(String tagsSql) {
    this.tagsSql = tagsSql;
  }
}
