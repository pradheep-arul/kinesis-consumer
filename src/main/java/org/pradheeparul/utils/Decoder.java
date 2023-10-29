package org.pradheeparul.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.kinesis.model.Record;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Decoder
 *
 * @author PradheepKumarA
 */
public class Decoder {
    private static final Logger logger = LogManager.getLogger(Decoder.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static List<Map<String, String>> decodeRecords(List<Record> records, String shardId) {
        List<Map<String, String>> result = new ArrayList<>();
        for (Record record : records) {
            byte[] byteArray = record.data().asByteArray();
            String s = new String(byteArray, StandardCharsets.UTF_8);
            try {
                Map<String, String> map = objectMapper.readValue(s, Map.class);
                map.put("shardId", shardId);
                map.put("sequenceNumber", record.sequenceNumber());
                map.put("hashCode", HashUtils.getHashCode(byteArray));
                result.add(map);
            } catch (IOException e) {
                logger.error("Error decoding record", e);
                throw new RuntimeException("Error decoding record", e);
            }
        }

        return result;
    }
}
