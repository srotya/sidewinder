/** 
 * 	Copyright 2016-2018 Michael Burman and/or other contributors.
*
*   Licensed under the Apache License, Version 2.0 (the "License");
*   you may not use this file except in compliance with the License.
*   You may obtain a copy of the License at
*
*       http://www.apache.org/licenses/LICENSE-2.0
*
*   Unless required by applicable law or agreed to in writing, software
*   distributed under the License is distributed on an "AS IS" BASIS,
*   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*   See the License for the specific language governing permissions and
*   limitations under the License.
**/  
package com.srotya.sidewinder.core.storage.compression.gorilla;


/**
 * Value decompressor for Gorilla encoded values
 *
 * @author Michael Burman
 */
public class ValueDecompressor {
    private BitInput in;
    private Predictor predictor;

    private int storedLeadingZeros = Integer.MAX_VALUE;
    private int storedTrailingZeros = 0;

    public ValueDecompressor(BitInput input) {
        this(input, new LastValuePredictor());
    }

    public ValueDecompressor(BitInput input, Predictor predictor) {
        this.in = input;
        this.predictor = predictor;
    }

    public long readFirst() {
        long value = in.getLong(Long.SIZE);
        predictor.update(value);
        return value;
    }

    public long nextValue() {
        int val = in.nextClearBit(2);

        switch(val) {
            case 3:
                // New leading and trailing zeros
                storedLeadingZeros = (int) in.getLong(6);

                byte significantBits = (byte) in.getLong(6);
                significantBits++;

                storedTrailingZeros = Long.SIZE - significantBits - storedLeadingZeros;
                // missing break is intentional, we want to overflow to next one
            case 2:
                long value = in.getLong(Long.SIZE - storedLeadingZeros - storedTrailingZeros);
                value <<= storedTrailingZeros;

                value = predictor.predict() ^ value;
                predictor.update(value);
                return value;
        }
        return predictor.predict();
    }
}
