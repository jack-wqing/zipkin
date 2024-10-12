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
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

public class SelectAutocompleteValues implements Function<ClickHouseConnection, List<String>> {
  final String spanTable;
  final String term;
  final long namesLookback;
  public SelectAutocompleteValues(String spanTable, String term, long namesLookback) {
    this.spanTable = spanTable;
    this.term = term;
    this.namesLookback = namesLookback;
  }

  @Override
  public List<String> apply(ClickHouseConnection connection) {
    if (StringUtils.isBlank(term)) {
      return Collections.emptyList();
    }
    long endMillis = System.currentTimeMillis();
    long beginMillis = endMillis - namesLookback;
    Set<String> tagValues = Sets.newHashSet();
    try (PreparedStatement statement = connection.prepareStatement(String.format(Constants.TAG_VALUE_SQL, term, spanTable))) {
      statement.setObject(1, DateFormatUtils.format(new Date(beginMillis), Constants.DATE_FORMAT));
      statement.setObject(2, DateFormatUtils.format(new Date(endMillis), Constants.DATE_FORMAT));
      ResultSet resultSet = statement.executeQuery();
      while (resultSet.next()) {
        String termValue = resultSet.getString(term);
        if (StringUtils.isNotBlank(termValue)) {
          tagValues.add(termValue);
        }
      }
    } catch (SQLException e) {
      Call.propagateIfFatal(e);
    }
    return Lists.newArrayList(tagValues);
  }
}
