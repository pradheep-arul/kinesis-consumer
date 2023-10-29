package org.pradheeparul.utils;

import com.sangupta.murmur.Murmur3;

public class HashUtils {
    private static final long RANDOM_HASH_SEED1 = 232423423423L;
    private static final long RANDOM_HASH_SEED2 = 734234234234L;

    public static String getHashCode(byte[] byteArray) {
        /**
         * To minimize the likelihood of collisions, generate two hashes using
         * distinct seed values and concatenate them.
         */
        long hashCode1 = Murmur3.hash_x86_32(byteArray, byteArray.length, RANDOM_HASH_SEED1);
        long hashCode2 = Murmur3.hash_x86_32(byteArray, byteArray.length, RANDOM_HASH_SEED2);
        return hashCode1 + "-" + hashCode2;
    }
}
