package org.hmsccb.ms;

import org.hmsccb.util.Util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class SqlServer {

    public static final String connectionUrl =
	"jdbc:sqlserver://localhost:1433;database=PublicDatasets";

    public void executeSqlList(List<String> sqlList) throws Exception {
        for (String s: sqlList) {
            this.executeSql(s);  // successive calls for same effect as "go" after each statement
        }
    }

    // For query that does not return ResultSet
    public void executeSql(String sql) {
        try (Connection conn = DriverManager.getConnection(connectionUrl);
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    // For file containing query that does not return ResultSet
    public void executeSqlFile(String sqlFile) throws Exception {
        String sql = Util.readFile(sqlFile);
        this.executeSql(sql);
    }

    // Load CSV file into SQL Server table
    public void loadTable(MsTable msTable, String csvFile) {
        String windowsCsvFile = Util.windowsPath(csvFile);
        String sql = "bulk insert " + msTable.schemaName + "." + msTable.tableName + " "
                + "from '" + windowsCsvFile + "' "
                + "with (keepnulls, tablock, firstrow=2, fieldterminator=',', rowterminator='0x0A')";

        this.executeSql(sql);
    }
}

