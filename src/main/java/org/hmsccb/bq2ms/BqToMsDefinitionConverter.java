package org.hmsccb.bq2ms;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import org.hmsccb.bq.BigQueryTable;
import org.hmsccb.ms.MsTable;
import org.hmsccb.util.Util;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Methods to generate T-SQL table definition from BigQuery table fields
public class BqToMsDefinitionConverter {

    // Mapping between BigQuery data types and SQL Server types (some are not yet attempted here, e.g., Array)
    private static Map<String, String> typeMap = new HashMap<String, String>();
    static {
        typeMap.put("float64", "float");
        typeMap.put("bool", "varchar");
        typeMap.put("byteint", "int");
        typeMap.put("int64", "bigint");
        typeMap.put("smallint", "int");
        typeMap.put("string", "text");
    }

    // Return SQL Server data type, given a BigQuery data type
    private static String getMsType(String type) {
        String newType = typeMap.get(type);
        if (newType == null) {
            return type;
        }
        return newType;
    }

    // Generate T-SQL commands to create SQL Server table
    public static MsTable convert(BigQueryTable bqTable) throws Exception {

        String datasetId = bqTable.datasetId;
        String tableId = bqTable.tableId;

        // Initialize SQL Server table with schema namespace and table name
        MsTable msTable = new MsTable(datasetId, tableId);

        // SQL commands to create schema namespace and drop any existing table:
        List<String> commands = new ArrayList<String>(
            Arrays.asList(
                "if not exists (select * from sys.schemas where name = N'" + datasetId + "')"
                    + "exec('create schema [" + datasetId + "]');",
                "drop table if exists " + datasetId + "." + tableId + ";"
            )
        );

        // Loop through BigQuery table fields and build create-table T-SQL command
        String command = "create table " + datasetId + "." + tableId + " (\n";
        FieldList fieldList = bqTable.getFieldList();
        for (int i = 0; i < fieldList.size(); i++) {
            Field field = fieldList.get(i);
            String fieldType = field.getType().getStandardType().toString().toLowerCase();
            command += "    " + field.getName() + " " + BqToMsDefinitionConverter.getMsType(fieldType);
            if (i < fieldList.size() - 1) {
                command += ",\n";
            }
        }
        command += ");";
        commands.add(command);
        msTable.tSqlList = commands;

        // Get directory where file should be written; create directory if needed
        File appWorkDir = Util.getWorkDir();
        File datasetDir = new File(appWorkDir.toString() + "/" + bqTable.datasetId);
        if (! datasetDir.exists()) {
            datasetDir.mkdirs();
        }

        msTable.tSqlFile = datasetDir.toString() + "/" + bqTable.tableId + ".sql";

        // Write T-SQL commands to file
        try(PrintStream ps = new PrintStream(msTable.tSqlFile)) {
            ps.print(String.join("\n", msTable.tSqlList));
        } catch (IOException e){
            e.printStackTrace();
            System.exit(1);
        }

        return msTable;
    }

}

