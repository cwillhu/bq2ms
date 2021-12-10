package org.hmsccb.test;

import org.hmsccb.ms.SqlServer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class SqlServerTest {
    @Test
    @DisplayName("Should read and write from SQL Server")
    void shouldReadWriteToSqlServer() throws Exception {
        // Write and read a random number to Sql Server test table, then test for equality.

        SqlServer sqlServer = new SqlServer();

        int randInt = ThreadLocalRandom.current().nextInt(0, 1_000_000);

        List<String> sqlList = Arrays.asList(
                "drop table if exists cwtest;",
                "create table cwtest (x int);",
                "insert into cwtest values (" + randInt + ")"
        );

        for (String s: sqlList) {
            sqlServer.executeSql(s); // equivalent of a "go" after each statement
        }

        int resultInt = -1; // outside randInt range

        try (Connection conn = DriverManager.getConnection(sqlServer.connectionUrl);
             PreparedStatement stmt = conn.prepareStatement("use PublicDatasets; select * from cwtest")) {
            ResultSet rs = stmt.executeQuery();;
            rs.next();
            resultInt = rs.getInt("x");
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }

        Assertions.assertEquals(resultInt, randInt);

        sqlServer.executeSql("drop table cwtest;");

    }
}
