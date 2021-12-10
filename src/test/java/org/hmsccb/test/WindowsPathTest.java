package org.hmsccb.test;

import org.hmsccb.util.Util;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class WindowsPathTest {

    @Test
    @DisplayName("Should create Windows path")
    void shouldCreateWindowsPath() {
        String macPath = "/Volumes/UserTemp$/some/path";
        String windowsPath = Util.windowsPath(macPath);

        Assertions.assertEquals(windowsPath, "S:\\UserTemp\\some\\path");
    }

}
