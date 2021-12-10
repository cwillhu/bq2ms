package org.hmsccb.ms;

import java.util.List;

// Container for SQL Server table properties
public class MsTable {

    public String schemaName;
    public String tableName;
    public List<String> tSqlList; // T-SQL commands for SQL Server table creation
    public String tSqlFile;  // File containing tSqlList commands

    public MsTable(String schemaName, String tableName) {
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

}
