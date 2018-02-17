/**
 * Copyright 2017 Ambud Sharma
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 		http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.srotya.sidewinder.core.storage.compression.zip;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;

import com.srotya.sidewinder.core.storage.compression.Codec;
import com.srotya.sidewinder.core.storage.compression.Reader;

@Codec(id = 3, name = "bzip")
public class BZip2Writer extends ZipWriter {

	@Override
	public Reader getReader() throws IOException {
		getRead().lock();
		ByteBuffer readBuf = buf.duplicate();
		getRead().unlock();
		readBuf.rewind();
		return new BZip2Reader(readBuf, getStartOffset(), getBlockSize());
	}

	@Override
	public OutputStream getOutputStream(ByteBufferOutputStream stream, int blockSize) throws IOException {
		return new BZip2CompressorOutputStream(stream, blockSize);
	}

	@Override
	public OutputStream getOutputStream(OutputStream stream, int blockSize) throws IOException {
		return new BZip2CompressorOutputStream(stream, blockSize);
	}

}
