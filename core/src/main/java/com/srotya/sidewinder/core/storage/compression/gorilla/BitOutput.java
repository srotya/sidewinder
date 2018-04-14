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

import java.io.IOException;

/**
 * This interface is used to write a compressed timeseries.
 *
 * @author Michael Burman
 */
public interface BitOutput {

    /**
     * Stores a single bit and increases the bitcount by 1
     * @throws IOException 
     */
    void writeBit() throws IOException;

    /**
     * Stores a 0 and increases the bitcount by 1
     * @throws IOException 
     */
    void skipBit() throws IOException;

    /**
     * Write the given long value using the defined amount of least significant bits.
     *
     * @param value The long value to be written
     * @param bits How many bits are stored to the stream
     * @throws IOException 
     */
    void writeBits(long value, int bits) throws IOException;

    /**
     * Flushes the current byte to the underlying stream
     * @throws IOException 
     */
    void flush() throws IOException;
}
