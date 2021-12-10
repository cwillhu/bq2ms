package org.hmsccb.bq;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import java.io.FileInputStream;
import java.util.Map;
import java.util.HashMap;

// Static methods to return single instance of BigQuery for any given projectId
public class BigQueryFactory {

    // Map projectId -> instance
    private static Map<String, BigQuery> map = new HashMap<String, BigQuery>();

    // Return instance of BigQuery
    public static synchronized BigQuery getInstance(String projectId) throws Exception {

        // Return appropriate BigQuery instance, if it exists
        if (map.containsKey(projectId)) {
            return map.get(projectId);
        }

        // Get location of API key
        final String credLocation = System.getenv("GOOGLE_APPLICATION_CREDENTIALS");
        if (credLocation == null) {
            throw new Exception("Environment variable GOOGLE_APPLICATION_CREDENTIALS must be set.");
        }

        // Create new BigQuery instance using projectId and API key
        BigQuery instance = BigQueryOptions
            .newBuilder()
            .setProjectId(projectId)
            .setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream(credLocation)))
            .build()
            .getService();

        // Add new instance to map
        map.put(projectId, instance);

        return instance;
    }

}
