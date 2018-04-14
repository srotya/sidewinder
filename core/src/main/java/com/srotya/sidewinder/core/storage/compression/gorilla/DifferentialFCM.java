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
 * Differential Finite Context Method (DFCM) is a context based predictor.
 *
 * @author Michael Burman
 */
public class DifferentialFCM implements Predictor {

    private long lastValue = 0L;
    private long[] table;
    private int lastHash = 0;

    private final int mask;

    /**
     * Create a new DFCM predictor
     *
     * @param size Prediction table size, will be rounded to the next power of two and must be larger than 0
     */
    public DifferentialFCM(int size) {
        if(size > 0) {
            size--;
            int leadingZeros = Long.numberOfLeadingZeros(size);
            int newSize = 1 << (Long.SIZE - leadingZeros);

            this.table = new long[newSize];
            this.mask = newSize - 1;
        } else {
            throw new IllegalArgumentException("Size must be positive and a power of two");
        }
    }

    @Override
    public void update(long value) {
        table[lastHash] = value - lastValue;
        lastHash = (int) (((lastHash << 5) ^ ((value - lastValue) >> 50)) & this.mask);
        lastValue = value;
    }

    @Override
    public long predict() {
        return table[lastHash] + lastValue;
    }
}
