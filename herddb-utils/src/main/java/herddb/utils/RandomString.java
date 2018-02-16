/*
 Licensed to Diennea S.r.l. under one
 or more contributor license agreements. See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership. Diennea S.r.l. licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.

 */
package herddb.utils;

import java.util.Random;

/**
 * Simple facilit to generate random strings for tests
 *
 * @author diego.salvi
 */
public class RandomString {

    private static final char[] ALPHABET = new char[] {
            '1','2','3','4','5','6','7','8','9','0',
            'A','B','C','D','E','F','G','H','I','J','K','L','M',
            'N','O','P','Q','R','S','T','U','V','W','X','Y','Z',
            'a','b','c','d','e','f','g','h','i','j','k','l','m',
            'n','o','p','q','r','s','t','u','v','w','x','y','z'};

    private static final RandomString INSTANCE = new RandomString(new Random(),ALPHABET);

    public static final RandomString getInstance() {
        return INSTANCE;
    }

    private char[] alphabet;

    private final Random random;

    /**
     * Build a new {@link RandomString} with a specific alphabet.
     */
    public RandomString(char[] alphabet) {
        this(INSTANCE.random,alphabet);
    }

    /**
     * Build a new {@link RandomString} with a specific random generator, useful when repeatable random
     * sequences are needed.
     */
    public RandomString(Random random) {
        this(random,ALPHABET);
    }

    /**
     * Build a new {@link RandomString} with a specific random generator, useful when repeatable random
     * sequences are needed.
     */
    public RandomString(Random random, char[] alphabet) {
        super();
        this.random   = random;
        this.alphabet = alphabet;
    }


    public String nextString(int len) {
        return nextString(len, new StringBuilder(len)).toString();
    }

    public String nextString(int min, int max) {
        int len = min == max ? min : random.nextInt(max - min) + min;
        return nextString(len);
    }

    public StringBuilder nextString(int len, StringBuilder builder) {
        for(int i = 0; i < len; ++i) {
            builder.append(alphabet[random.nextInt(alphabet.length)]);
        }
        return builder;
    }

    public StringBuilder nextString(int min, int max, StringBuilder builder) {
        int len = min == max ? min : random.nextInt(max - min) + min;
        return nextString(len,builder);
    }

}
