package zipkin2.clickhouse;

import com.clickhouse.jdbc.ClickHouseConnection;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
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
public class SelectServiceSpanName implements Function<ClickHouseConnection, List<String>> {
  private final String spanTable;
  private long namesLookback;
  private String serviceName;
  private String term;

  public SelectServiceSpanName(String spanTable, long namesLookback, String serviceName, String term) {
    this.spanTable = spanTable;
    this.namesLookback = namesLookback;
    this.serviceName = serviceName;
    this.term = term;
  }

  @Override
  public List<String> apply(ClickHouseConnection connection) {
    long endMillis = System.currentTimeMillis();
    long beginMillis = endMillis - namesLookback;
    Set<String> serviceNames = Sets.newHashSet();
    try (PreparedStatement statement = connection.prepareStatement(String.format(Constants.SERVICE_TERM_SQL, term, spanTable))) {
      statement.setObject(1, serviceName);
      statement.setObject(2, DateFormatUtils.format(new Date(beginMillis), Constants.DATE_FORMAT));
      statement.setObject(3, DateFormatUtils.format(new Date(endMillis), Constants.DATE_FORMAT));
      ResultSet resultSet = statement.executeQuery();
      while (resultSet.next()) {
        String serviceName = resultSet.getString(term);
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
