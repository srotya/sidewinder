/**
 * Copyright Ambud Sharma
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
package com.srotya.sidewinder.tools;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import com.srotya.sidewinder.core.storage.disk.DiskMalloc;
import com.srotya.sidewinder.core.utils.MiscUtils;

/**
 * Utility to read PTR files and print in a human readable format
 * 
 * @author ambud
 */
public class PTRFileReader {

	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			System.err.println("Argument needed with path of PTR file to read");
			System.exit(-1);
		}

		File file = new File(args[0]);
		RandomAccessFile raf = new RandomAccessFile(file, "r");
		FileChannel ch = raf.getChannel();
		MappedByteBuffer mBuf = ch.map(MapMode.READ_ONLY, 0, file.length());

		ch.close();
		raf.close();

		int ptrCounter = mBuf.getInt();

		for (int i = 0; i < ptrCounter; i++) {
			String line = MiscUtils.getStringFromBuffer(mBuf);
			String[] splits = line.split("\\" + DiskMalloc.SEPARATOR);
			String fileName = splits[1];
			int positionOffset = Integer.parseInt(splits[3]);
			String seriesIdStr = splits[0];
			int pointer = Integer.parseInt(splits[2]);
			int size = Integer.parseInt(splits[4]);
			System.out.println(seriesIdStr + "," + fileName + "," + pointer + "," + positionOffset + "," + size);
		}

	}

}
