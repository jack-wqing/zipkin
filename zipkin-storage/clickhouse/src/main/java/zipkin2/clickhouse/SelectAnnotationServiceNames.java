package zipkin2.clickhouse;

import com.clickhouse.jdbc.ClickHouseConnection;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import zipkin2.Call;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * clickhouse: 通过时间窗口控制服务名
 */
public class SelectAnnotationServiceNames implements Function<ClickHouseConnection, List<String>> {
  private final String spanTable;
  private long namesLookback;

  public SelectAnnotationServiceNames(String spanTable, long namesLookback) {
    this.spanTable = spanTable;
    this.namesLookback = namesLookback;
  }

  @Override
  public List<String> apply(ClickHouseConnection connection) {

    long endMillis = System.currentTimeMillis();
    long beginMillis = endMillis - namesLookback;
    Set<String> serviceNames = Sets.newHashSet();
    try (PreparedStatement statement = connection.prepareStatement(String.format(Constants.SERVICE_SQL, spanTable))) {
      statement.setObject(1, DateFormatUtils.format(new Date(beginMillis), Constants.DATE_FORMAT));
      statement.setObject(2, DateFormatUtils.format(new Date(endMillis), Constants.DATE_FORMAT));
      ResultSet resultSet = statement.executeQuery();
      while (resultSet.next()) {
        String serviceName = resultSet.getString("localEndpointServiceName");
        if (StringUtils.isNotBlank(serviceName)) {
          serviceNames.add(serviceName);
        }
      }
    } catch (SQLException e) {
      Call.propagateIfFatal(e);
    }
    return Lists.newArrayList(serviceNames);

  }
}
