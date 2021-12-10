package org.hmsccb.util;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import org.hmsccb.bq.BigQueryFactory;

// Create a BigQuery dataset (the BigQuery equivalent to a Postgres or SQL Server schema)
public class CreateDataset {
    public static void main(String[] args) throws Exception {
        String datasetName = "cw_scratch_dataset";
        DatasetInfo datasetInfo = DatasetInfo.newBuilder(datasetName).build();

        BigQuery bigquery = BigQueryFactory.getInstance("cwproj");
        Dataset dataset = bigquery.create(datasetInfo);

        System.out.printf("Dataset %s created.\n", dataset.getDatasetId().getDataset());
    }
}