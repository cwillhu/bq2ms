package org.hmsccb.bq2ms;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;
import java.util.concurrent.Callable;

// Command-line interface for BigQueryToSqlServer
@Command(name = "java -jar <path to bq2ms.jar>", mixinStandardHelpOptions = true, version = "bq2ms 0.1",
        description = "Transfer BigQuery table to SQL Server")
class Main implements Callable<Integer> {
    // Reference : https://picocli.info/

    @Parameters(index = "0", description = "BigQuery dataset name")
    private String datasetId;

    @Parameters(index = "1", description = "BigQuery table name.")
    private String tableId;

    private String projectId = "bigquery-public-data";

    @Override
    public Integer call() throws Exception {
        BigQueryToSqlServer.transferTable(projectId, datasetId, tableId);
        return 0;
    }

    public static void main(String... args) {
        int exitCode = new CommandLine(new Main()).execute(args);
        System.exit(exitCode);
    }
}
