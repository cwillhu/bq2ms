package org.hmsccb.bq2ms;

import org.hmsccb.bq.BigQueryTable;
import org.hmsccb.gcs.Bucket;
import org.hmsccb.ms.MsTable;
import org.hmsccb.ms.SqlServer;

// Transfer BigQuery table to SQL Server
public class BigQueryToSqlServer {

    public static void transferTable(String projectId, String datasetId, String tableId) {

        try {
            BigQueryTable bqTable = new BigQueryTable(projectId, datasetId, tableId);
            System.out.println("Starting transfer of BigQuery table: " + datasetId + "." + tableId);

            // Generate T-SQL to define SQL Server table
            MsTable msTable = BqToMsDefinitionConverter.convert(bqTable);

            // Execute T-SQL to create SQL Server table
            SqlServer sqlServer = new SqlServer();
            sqlServer.executeSqlList(msTable.tSqlList);

            // Export BigQuery table to Google Cloud bucket
            Bucket bucket = new Bucket();
            bqTable.exportTo(bucket);

            // Download BigQuery CSV from bucket to smb share
            String csvFile = bucket.downloadCsv(bqTable.storageObjectName);

            // Load BigQuery CSV into SQL Server
            sqlServer.loadTable(msTable, csvFile);

            System.out.println("Transfer complete.");

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

    }

}
