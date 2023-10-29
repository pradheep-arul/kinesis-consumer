package org.pradheeparul.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Kinesis
 *
 * @author PradheepKumarA
 */
public class Kinesis {
    private static final Logger logger = LogManager.getLogger(Kinesis.class);

    public static List<String> getShards() {
        List<String> shardIds = new ArrayList<>();
        KinesisAsyncClient client = KinesisAsyncClient.builder().build();
        try {
            ListShardsRequest request = ListShardsRequest.builder()
                    .streamName(Constants.STREAM_NAME)
                    .build();

            ListShardsResponse response = client.listShards(request).get(5000, TimeUnit.MILLISECONDS);
            for (Shard shard : response.shards()) {
                shardIds.add(shard.shardId());
            }
        } catch (Exception e) {
            logger.error("Error while getting shard IDs: " + e.getMessage());
        } finally {
            client.close();
        }
        return shardIds;
    }


    public static List<Record> retrieveRecords(String shardId, String lastSequenceNumber) {
        String shardIterator = Kinesis.getShardIterator(shardId, lastSequenceNumber);

        KinesisClient client = KinesisClient.builder().build();
        GetRecordsRequest getRecordsRequest = GetRecordsRequest.builder()
                .shardIterator(shardIterator)
                .limit(Constants.RECORDS_FETCH_LIMIT)
                .build();

        GetRecordsResponse result = client.getRecords(getRecordsRequest);
        return result.records();
    }


    public static String getShardIterator(String shardId, String lastSequenceNumber) {
        KinesisClient client = KinesisClient.builder().build();
        // TODO: Set ShardIteratorType.TRIM_HORIZON and ShardIteratorType.AFTER_SEQUENCE_NUMBER
        GetShardIteratorRequest itrRequest = GetShardIteratorRequest.builder()
                .streamName(Constants.STREAM_NAME)
                .shardIteratorType(lastSequenceNumber != null ? ShardIteratorType.AFTER_SEQUENCE_NUMBER : ShardIteratorType.TRIM_HORIZON)
                .startingSequenceNumber(lastSequenceNumber)
                .shardId(shardId)
                .build();
        GetShardIteratorResponse shardItrRes = client.getShardIterator(itrRequest);
        return shardItrRes.shardIterator();
    }
}
