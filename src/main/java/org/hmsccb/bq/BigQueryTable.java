package org.hmsccb.bq;

import com.google.cloud.RetryOption;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import org.threeten.bp.Duration;

import org.hmsccb.gcs.Bucket;

public class BigQueryTable {

    public String tableProjectId, datasetId, tableId, storageObjectName, ccbProjectId;

    public BigQueryTable(String tableProjectId, String datasetId, String tableId) {
        this.tableProjectId = tableProjectId;
        this.datasetId = datasetId;
        this.tableId = tableId;
        this.storageObjectName = datasetId + "/" + tableId;
        this.ccbProjectId = "cwproj";
    }

    // Return field definitions of BigQuery table
    public FieldList getFieldList() throws Exception {
        FieldList fieldList = null;
        try {
            BigQuery bigquery = BigQueryFactory.getInstance(this.tableProjectId);
            Table table = bigquery.getTable(this.datasetId, this.tableId);
            Schema schema = table.getDefinition().getSchema();
            fieldList = schema.getFields();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return fieldList;
    }

    // Export BigQuery table to CSV in cloud storage
    public void exportTo(Bucket bucket) throws Exception {
        // Set GCS location where CSV will be saved
        String destinationUri = "gs://" + bucket.bucketName + "/" + this.datasetId + "/" + this.tableId;
        String dataFormat = "CSV";

        try {
            BigQuery bigquery = BigQueryFactory.getInstance(this.ccbProjectId);
            TableId tId = TableId.of(this.tableProjectId, this.datasetId, this.tableId);
            Table table = bigquery.getTable(tId);

            Job job = table.extract(dataFormat, destinationUri);

            // Blocks until job completes
            Job completedJob =
                    job.waitFor(
                            RetryOption.initialRetryDelay(Duration.ofSeconds(1)),
                            RetryOption.totalTimeout(Duration.ofMinutes(3)));

            if (completedJob == null) {
                System.err.println("Job not executed since it no longer exists.");
                System.exit(1);
            } else if (completedJob.getStatus().getError() != null) {
                System.err.println("BigQuery was unable to extract due to an error: \n" + job.getStatus().getError());
                System.exit(1);
            }

        } catch (BigQueryException | InterruptedException e) {
            System.err.println("Table extraction failed: " + e.toString());
            System.exit(1);
        }
    }

}
