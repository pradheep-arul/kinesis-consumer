package com.pradheeparul.starter;

import com.pradheeparul.KinesisConsumerWorker;

/**
 * Main
 *
 * @author PradheepKumarA
 */
public class Starter {
    public static void main(String[] args) {
        new KinesisConsumerWorker().initiate();
    }
}
