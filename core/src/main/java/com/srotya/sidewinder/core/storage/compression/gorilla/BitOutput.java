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
