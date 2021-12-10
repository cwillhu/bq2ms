package org.hmsccb.gcs;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.hmsccb.util.Util;

import java.io.File;
import java.nio.file.Paths;

// Google cloud storage bucket
public class Bucket {

    public String bucketName, storageProjectId;

    // Defaults for storage project and bucket name
    public Bucket() {
        this("cwproj", "org-hmsccb-bucket1");
    }

    public Bucket(String storageProjectId, String bucketName) {
        this.storageProjectId = storageProjectId;
        this.bucketName = bucketName;
    }

    // Download CSV object from cloud storage
    public String downloadCsv(String objectName) {

        // Verify that env var for API key is set
        final String credLocation = System.getenv("GOOGLE_APPLICATION_CREDENTIALS");
        if (credLocation == null) {
            System.err.println("Environment variable GOOGLE_APPLICATION_CREDENTIALS must be set.");
            System.exit(1);
        }

        Storage storage = StorageOptions.newBuilder().setProjectId(this.storageProjectId).build().getService();
        Blob blob = storage.get(BlobId.of(this.bucketName, objectName));

        // Get location where file will be written
        File appWorkDir = Util.getWorkDir();
        String destFilePath = appWorkDir.toString() + "/" + objectName + ".csv";

        // Download CSV file
        blob.downloadTo(Paths.get(destFilePath));

        return destFilePath;
    }
}
