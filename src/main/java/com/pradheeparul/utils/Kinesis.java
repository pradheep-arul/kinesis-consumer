package com.pradheeparul.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;

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
    private static KinesisClient client;
    private static KinesisAsyncClient asyncClient;

    public static KinesisClient getClient() {
        if (client == null) {
            client = KinesisClient.builder().build();
        }
        return client;
    }

    public static KinesisAsyncClient getAsyncClient() {
        if (asyncClient == null) {
            asyncClient = KinesisAsyncClient.builder().build();
        }
        return asyncClient;
    }

    public static List<String> getShards() {
        List<String> shardIds = new ArrayList<>();
        try {
            ListShardsRequest request = ListShardsRequest.builder()
                    .streamName(Constants.STREAM_NAME)
                    .build();

            ListShardsResponse response = getAsyncClient().listShards(request).get(5000, TimeUnit.MILLISECONDS);
            for (Shard shard : response.shards()) {
                shardIds.add(shard.shardId());
            }
        } catch (Exception e) {
            logger.error("Error while getting shard IDs: " + e.getMessage());
        }
        return shardIds;
    }


    public static List<Record> retrieveRecords(String shardId, String lastSequenceNumber) {
        String shardIterator = Kinesis.getShardIterator(shardId, lastSequenceNumber);
        GetRecordsRequest getRecordsRequest = GetRecordsRequest.builder()
                .shardIterator(shardIterator)
                .limit(Constants.RECORDS_FETCH_LIMIT)
                .build();

        GetRecordsResponse result = getClient().getRecords(getRecordsRequest);
        return result.records();
    }


    public static String getShardIterator(String shardId, String lastSequenceNumber) {
        GetShardIteratorRequest itrRequest = GetShardIteratorRequest.builder()
                .streamName(Constants.STREAM_NAME)
                .shardIteratorType(lastSequenceNumber != null ? ShardIteratorType.AFTER_SEQUENCE_NUMBER : ShardIteratorType.TRIM_HORIZON)
                .startingSequenceNumber(lastSequenceNumber)
                .shardId(shardId)
                .build();
        GetShardIteratorResponse shardItrRes = getClient().getShardIterator(itrRequest);
        return shardItrRes.shardIterator();
    }
}
