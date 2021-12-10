package org.hmsccb.util;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class Util {

    public static File getWorkDir() {
        // Get application's work directory
        final String dirString = System.getenv("BQ2MS_WORK_DIR");
        if (dirString == null) {
            System.err.println("Environment variable BQ2MS_WORK_DIR must be set.");
            System.exit(1);
        }

        // Create work directory if it doesn't exist
        File appWorkDir = new File(dirString);
        if (! appWorkDir.exists()) {
            appWorkDir.mkdirs();
        }
        return appWorkDir;
    }

    public static String readFile(String filePath) throws IOException {
        File file = new File(filePath);
        return FileUtils.readFileToString(file, StandardCharsets.UTF_8);
    }

    public static String windowsPath(String filePath) {
        return filePath
                .replaceFirst("/Volumes/UserTemp[$]/", "S:\\\\UserTemp\\\\")
                .replaceAll("/", "\\\\");
    }
}
