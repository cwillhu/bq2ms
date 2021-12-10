package org.hmsccb.test;

import com.google.cloud.bigquery.FieldList;
import org.hmsccb.bq.BigQueryTable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class BigQueryTest {
    @Test
    @DisplayName("Should read BigQuery table properties")
    void shouldReadBigQueryTableProperties() {
        String projectId = "bigquery-public-data";
        String datasetId = "census_bureau_acs";
        String tableId = "state_2018_1yr";

        try {
            BigQueryTable bqTable = new BigQueryTable(projectId, datasetId, tableId);
            FieldList fieldList = bqTable.getFieldList();
            String firstFieldName = fieldList.get(0).getName();

            Assertions.assertEquals(fieldList.size(), 252);
            Assertions.assertEquals(firstFieldName, "geo_id");

        } catch (Exception e) {
            Assertions.fail(e.toString());
        }
    }
}
