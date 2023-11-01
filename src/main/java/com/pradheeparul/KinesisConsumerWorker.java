package com.pradheeparul;

import com.pradheeparul.metrics.MetricProcessor;
import com.pradheeparul.utils.DeDuplicator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.pradheeparul.storage.DiskStorage;
import com.pradheeparul.storage.Storage;
import com.pradheeparul.utils.Cache;
import com.pradheeparul.utils.Decoder;
import com.pradheeparul.utils.Kinesis;
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

    public void initiate() {
        logger.info("Initiating KinesisConsumerWorker");
        while (true) {
            // Retrieve the list of available shard IDs each time
            // to dynamically manage shard additions and deletions in Kinesis.
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

        // Choose the storage mechanism. In this project, we are using disk storage in files
        // as opposed to storing in an ideal S3 location.
        Storage storage = new DiskStorage();

        // Step 1: Retrieve records from Kinesis.
        List<Record> encodedRecords = fetchRecordsFromKinesis(shardId);

        // Step 2: Decode the records.
        List<Map<String, String>> decodedRecords = Decoder.decodeRecords(encodedRecords, shardId);

        // Step 3: Store the decoded records in permanent storage.
        // This is useful for evaluating the deduplication behavior in case of any issues.
        storage.store(shardId, decodedRecords, "raw-records");

        // Step 4: Remove duplicates from records
        DeDuplicator deDuplicator = new DeDuplicator();
        List<Map<String, String>> deduplicateRecords = deDuplicator.deduplicate(decodedRecords, shardId);
        
        // Step 5: Persist deduplicated records in permanent storage for potential batch processing in the future.
        storage.store(shardId, deduplicateRecords, "dedup-records");

        // Step 6: Process and compute predefined metrics
        metricProcessor.processMetrics(deduplicateRecords);

        // Step 7: Log computed metrics to the console,
        // Ideal storing them in a data warehouse or sending to Datadog.
        metricProcessor.logMetrics();

        // Step 8: Mark these records as processed in the Cache
        deDuplicator.markRecordsProcessed(deduplicateRecords);

        // Step 9: Update the last sequence number in cache for next iteration in this shard
        updateLastSequenceNumberInCache(shardId, deduplicateRecords);
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