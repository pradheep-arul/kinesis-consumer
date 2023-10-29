package org.pradheeparul.starter;

import org.pradheeparul.KinesisConsumerWorker;

/**
 * Main
 *
 * @author PradheepKumarA
 */
public class Main {
    public static void main(String[] args) {
        new KinesisConsumerWorker().start();
    }
}
