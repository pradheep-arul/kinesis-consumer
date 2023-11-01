package com.pradheeparul.storage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/**
 * DiskStorage
 *
 * @author PradheepKumarA
 */
public class DiskStorage implements Storage {
    private final Logger logger = LogManager.getLogger(DiskStorage.class);
    private final String LOGS_BASE_PATH = "./logs/";

    public static LocalDateTime roundDownToNearest5Minutes() {
        LocalDateTime time = LocalDateTime.now();
        return time.minusMinutes(time.getMinute() % 5).withSecond(0).withNano(0);
    }

    public void store(String shardId, List<Map<String, String>> records, String prefix) {
        String filepath = LOGS_BASE_PATH + prefix + "-" + roundDownToNearest5Minutes() + ".log";

        try (FileWriter myWriter = new FileWriter(filepath, true)) {
            for (Map<String, String> record : records) {
                String logMessage = String.format("%s : %s : %s \n", LocalDateTime.now(), shardId, record);
                myWriter.write(logMessage);
            }
        } catch (IOException e) {
            logger.error("Error writing to disk storage: " + e.getMessage(), e);
        }
    }
}
