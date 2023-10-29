package org.pradheeparul;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.pradheeparul.metrics.MetricProcessor;
import org.pradheeparul.storage.DiskStorage;
import org.pradheeparul.storage.Storage;
import org.pradheeparul.utils.Cache;
import org.pradheeparul.utils.DeDuplicator;
import org.pradheeparul.utils.Decoder;
import org.pradheeparul.utils.Kinesis;
import software.amazon.awssdk.services.kinesis.model.Record;

import java.util.List;
import java.util.Map;

/**
 * KinesisConsumerWorker
 *
 * @author PradheepKumarA
 */
public class KinesisConsumerWorker {
    private static final Logger logger = LogManager.getLogger(KinesisConsumerWorker.class);

    public void start() {
        logger.info("Initiating KinesisConsumerWorker");
        while (true) {
            List<String> shardIds = Kinesis.getShards();
            logger.info("Retrieved shardIds: " + shardIds);
            for (String shardId : shardIds) {
                processShard(shardId);
            }
        }
    }

    public void processShard(String shardId) {
        logger.info("Processing Shard: " + shardId);
        MetricProcessor metricProcessor = new MetricProcessor(shardId);

        // Choose the storage mechanism
        Storage storage = new DiskStorage();

        // Step 1: Retrieve records from Kinesis
        List<Record> encodedRecords = fetchRecordsFromKinesis(shardId);

        // Step 2: Decode the records
        List<Map<String, String>> decodedRecords = Decoder.decodeRecords(encodedRecords, shardId);

        // Step 3: Store decoded records in permanent storage
        storage.store(shardId, decodedRecords, "raw-records");

        // Step 4: Remove duplicate records
        List<Map<String, String>> deduplicatedRecords = DeDuplicator.deduplicate(decodedRecords);

        // Step 5: Process and compute metrics
        metricProcessor.processMetrics(deduplicatedRecords);

        // Step 6: Store deduplicated records in permanent storage for future batch processing
        storage.store(shardId, deduplicatedRecords, "dedup-records");

        // Step 7: Log computed metrics to the console
        metricProcessor.logMetrics();

        // Step 8: Cache hash codes of records for deduplication
        DeDuplicator.cacheHashCodes(deduplicatedRecords);

        // Step 9: Update the last sequence number in cache
        updateLastSequenceNumberInCache(shardId, deduplicatedRecords);
    }


    private List<Record> fetchRecordsFromKinesis(String shardId) {
        String cacheKey = getShardCacheKey(shardId);
        String lastSequenceNumber = Cache.get(cacheKey);
        logger.info("Retrieved last sequence number for shard " + shardId + " from cache: " + lastSequenceNumber);

        return Kinesis.retrieveRecords(shardId, lastSequenceNumber);
    }

    private void updateLastSequenceNumberInCache(String shardId, List<Map<String, String>> records) {
        if (!records.isEmpty()) {
            String cacheKey = getShardCacheKey(shardId);
            Cache.set(cacheKey, records.get(records.size() - 1).get("sequenceNumber"));
        }
    }

    public String getShardCacheKey(String shardId) {
        return "LAST_SEQUENCE_NUMBER-SHARD-" + shardId;
    }

}