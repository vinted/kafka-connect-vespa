package com.vinted.kafka.connect.vespa;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

public class VespaSinkConnectorVersion {
    private static final Logger log = LoggerFactory.getLogger(VespaSinkConnectorVersion.class);
    private static String version = "unknown";

    private static final String VERSION_FILE = "/kafka-connect-vespa-version.properties";

    static {
        try {
            Properties props = new Properties();
            try (InputStream versionFileStream = VespaSinkConnectorVersion.class.getResourceAsStream(VERSION_FILE)) {
                props.load(versionFileStream);
                version = props.getProperty("version", version).trim();
            }
        } catch (Exception e) {
            log.warn("Error while loading version:", e);
        }
    }

    public static String getVersion() {
        return version;
    }
}
