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
package com.srotya.minuteman.wal;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.logging.Logger;

/**
 * Write ahead log implementation with replication in mind with automated
 * compaction built-in. This WAL implementation has the concept of segments
 * where a segment represents a chunk to data that is successfully persisted.
 * 
 * Compaction implies that data replicated over by followers successfully can be
 * marked for deletion.
 * 
 * @author ambud
 */
public class MappedWAL implements WAL {

	private static final Logger logger = Logger.getLogger(MappedWAL.class.getName());
	private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	private ReadLock read = lock.readLock();
	private WriteLock write = lock.writeLock();
	private MappedByteBuffer currentWrite;
	private int segmentSize;
	private volatile int segmentCounter;
	private RandomAccessFile raf;
	private String walDirectory;
	private Map<Integer, MappedWALFollower> followerMap;
	private volatile int counter;
	private int maxCounter;
	private int isrThreshold;
	// TODO potential bug since commit file is not tracked
	private volatile long commitOffset = 0;
	private AtomicBoolean walDeletion;
	private RandomAccessFile metaraf;
	private MappedByteBuffer metaBuf;

	public MappedWAL() {
	}

	@Override
	public void configure(Map<String, String> conf, ScheduledExecutorService es) throws IOException {
		walDirectory = conf.getOrDefault(WAL_DIR, DEFAULT_WAL_DIR);
		logger.info("Configuring WAL directory:" + walDirectory);
		new File(walDirectory).mkdirs();
		// maximum size of a segment
		// each segment is memory mapped in whole; default's 100MB
		segmentSize = Integer.parseInt(conf.getOrDefault(WAL_SEGMENT_SIZE, String.valueOf(DEFAULT_WALL_SIZE)));
		logger.info("Configuring segment size:" + segmentSize);
		// maximum number of writes after which a force flush is triggered
		maxCounter = Integer.parseInt(conf.getOrDefault(WAL_SEGMENT_FLUSH_COUNT, String.valueOf(-1)));
		logger.info("Configuring max flush counter to:" + maxCounter);
		// offset threshold beyond which a follower is not considered an ISR
		isrThreshold = Integer.parseInt(conf.getOrDefault(WAL_ISR_THRESHOLD, String.valueOf(1024 * 1024 * 8)));
		// create an empty map of all followers
		followerMap = new ConcurrentHashMap<>();
		// disable wal deletion i.e. all WALs will be preserved even after it
		// is
		// read by the follower
		walDeletion = new AtomicBoolean(Boolean.parseBoolean(conf.getOrDefault(WAL_DELETION, "false")));
		logger.info("WAL deletion is set to:" + walDeletion.get());
		// isr polling thread
		es.scheduleAtFixedRate(() -> {
			updateISR();
		}, Integer.parseInt(conf.getOrDefault(WAL_ISRCHECK_DELAY, "1000")),
				Integer.parseInt(conf.getOrDefault(WAL_ISRCHECK_FREQUENCY, "10000")), TimeUnit.MILLISECONDS);

		// initialize metaraf
		initAndRecoverMetaRaf();
		// initialize the wal segments
		checkAndRotateSegment(0);
	}

	public void updateISR() {
		long currentOffset = (currentWrite.position() + (long) ((segmentCounter - 1) * segmentSize));
		for (Entry<Integer, MappedWALFollower> entry : followerMap.entrySet()) {
			MappedWALFollower follower = entry.getValue();
			Integer followerId = entry.getKey();
			if (currentOffset - follower.getOffset() > isrThreshold) {
				follower.setIsr(false);
				logger.fine("Follower no longer an ISR:" + followerId + " wal:" + walDirectory + " current offset:"
						+ currentOffset + " follower:" + follower.getOffset() + " thresholdDiff:"
						+ (currentOffset - follower.getOffset()) + " isrThreshold:" + isrThreshold);
			} else {
				follower.setIsr(true);
				logger.fine("Follower now an ISR:" + followerId + " wal:" + walDirectory + " with offset("
						+ getCommitOffset() + ")");
			}
		}
		updateMinOffset();
	}

	private void initAndRecoverMetaRaf() throws IOException {
		metaraf = new RandomAccessFile(walDirectory + "/.md", "rwd");
		boolean forward = false;
		if (metaraf.length() > 0) {
			forward = true;
		}
		metaBuf = metaraf.getChannel().map(MapMode.READ_WRITE, 0, 1024);
		if (forward) {
			logger.info("Found md file with commit offset");
			commitOffset = metaBuf.getLong();
		} else {
			metaBuf.putLong(0);
		}
	}

	@Override
	public void flush() throws IOException {
		write.lock();
		if (currentWrite != null) {
			logger.finer("Flushing buffer to disk");
			currentWrite.force();
		}
		write.unlock();
	}

	@Override
	public void close() throws IOException {
		write.lock();
		flush();
		if (raf != null) {
			raf.close();
		}
		for (Entry<Integer, MappedWALFollower> entry : followerMap.entrySet()) {
			if (entry.getValue().getReader() != null) {
				entry.getValue().getReader().close();
			}
		}
		write.unlock();
	}

	@Override
	public int getOffset() throws IOException {
		if (currentWrite != null) {
			return currentWrite.position();
		} else {
			return -1;
		}
	}

	@Override
	public void write(byte[] data, boolean flush) throws IOException {
		write.lock();
		checkAndRotateSegment(data.length + Integer.BYTES);
		try {
			counter++;
			currentWrite.putInt(data.length);
			currentWrite.put(data);
			currentWrite.putInt(0, currentWrite.position());
			if (maxCounter != -1 && (flush || counter == maxCounter)) {
				flush();
				counter = 0;
			}
			logger.finest("Wrote data:" + data.length + " at new offset:" + currentWrite.position() + " new file:"
					+ segmentCounter);
		} finally {
			write.unlock();
		}
	}

	@Override
	public WALRead read(Integer followerId, long requestOffset, int maxBytes, boolean readCommitted) throws IOException {
		read.lock();
		MappedWALFollower follower = followerMap.get(followerId);
		if (follower == null) {
			follower = new MappedWALFollower();
			followerMap.put(followerId, follower);
			logger.info("No follower entry, creating one for:" + followerId);
		}
		read.unlock();
		WALRead readData = new WALRead();
		int segmentId = rawOffsetToSegmentId(requestOffset);
		int followerSegmentId = rawOffsetToSegmentId(follower.getOffset());
		checkAndCleanupSegment(followerId, follower, segmentId, followerSegmentId);
		// if follower buffer is null; then initialize it
		if (follower.getBuf() == null) {
			WALRead read = initializeBuffer(follower, segmentId, followerId, readData);
			if (read != null) {
				return read;
			}
		}
		// initialize follower offset
		follower.setOffset(requestOffset);
		int pos = follower.getBuf().getInt(0);
		// bug fix where the committed offset read couldn't get anything since the min
		// offset was not yet computed even though it may have been updated by other
		// followers
		updateMinOffset();
		if (readCommitted && segmentId >= rawOffsetToSegmentId(commitOffset)) {
			// if there's a segment difference then get offset from the
			// current open segment
			pos = rawOffsetToSegmentOffset(commitOffset);
			logger.fine("Reading committed pos:" + pos + " segmentid:" + segmentId + " commit offset:" + commitOffset
					+ " req:" + (commitOffset - requestOffset));
		}
		// read data and set next offset
		readDataAndUpdateOffset(followerId, requestOffset, maxBytes, follower, readData, segmentId, pos, readCommitted);
		// logger.info("Commit offset:" + getCommitOffset());
		readData.setCommitOffset(getCommitOffset());
		return readData;
	}

	private void readDataAndUpdateOffset(Integer followerId, long requestOffset, int maxBytes,
			MappedWALFollower follower, WALRead readData, int segmentId, int pos, boolean readCommitted) {
		ByteBuffer buf = follower.getBuf();
		int offset = rawOffsetToSegmentOffset(requestOffset);
		buf.position(offset);
		if (offset < pos) {
			readDataTillPos(requestOffset, maxBytes, readData, segmentId, pos, buf, offset);
		}
		if (pos == buf.position() && segmentCounter - 1 > segmentId) {
			long next = (long) (segmentId + 1) * segmentSize + Integer.BYTES;
			logger.fine("Follower(" + followerId
					+ ") doesn't have any more data to read from this segment, incrementing segment(" + segmentId
					+ "); segmentCounter:" + segmentCounter + " offset(" + requestOffset + ") moving to next offset("
					+ next + ") pos(" + pos + ") bufpos(" + buf.position() + ")");
			readData.setNextOffset(next);
		} else {
			long next = (long) (segmentId) * segmentSize + buf.position();
			logger.fine("Follower(" + followerId + ") has more data to read from this(" + segmentId
					+ ") segment; moving to next offset(" + next + ") pos(" + pos + ") bufpos(" + buf.position() + ")");
			readData.setNextOffset(next);
		}
		if (readCommitted && readData.getNextOffset() > commitOffset) {
			// reset commit offset if it exceeded
			readData.setNextOffset(commitOffset);
		}
	}

	private void readDataTillPos(long requestOffset, int maxBytes, WALRead readData, int segmentId, int pos,
			ByteBuffer buf, int offset) {
		// int length = maxBytes < (pos - offset) ? maxBytes : (pos - offset);
		List<byte[]> data = new ArrayList<>();
		int length = 0;
		int temp = offset;
		do {
			buf.position(temp);
			int currPayloadLength = buf.getInt();
			byte[] dst = new byte[currPayloadLength];
			buf.get(dst);
			data.add(dst);
			length += currPayloadLength + Integer.BYTES;
			temp = offset + length;
		} while (length <= maxBytes && temp < pos);
		logger.finest("//error:" + offset + " l:" + length + " r:" + buf.remaining() + " t:" + temp + " r:"
				+ requestOffset + " s:" + segmentId + " p:" + pos + " s:" + data.size() + " b:" + buf.position());
		readData.setData(data);
	}

	private WALRead initializeBuffer(MappedWALFollower follower, int segmentId, Integer followerId, WALRead readData)
			throws IOException {
		File file = new File(getSegmentFileName(walDirectory, segmentId));
		if (!file.exists() && segmentId < segmentCounter) {
			long next = segmentId + 1 * segmentSize + Integer.BYTES;
			logger.warning("Follower(" + followerId + ") requested file(" + segmentId + "=" + file.getAbsolutePath()
					+ ") doesn't exist, incrementing next(" + next + ")");
			readData.setNextOffset(next);
			return readData;
		} else {
			logger.fine("Follower(" + followerId + "), opening new wal segment to read:" + file.getAbsolutePath());
			RandomAccessFile raf = new RandomAccessFile(file, "r");
			follower.setReader(raf);
			follower.setBuf(raf.getChannel().map(MapMode.READ_ONLY, 0, file.length()));
			// skip the header
			follower.getBuf().getInt();
			return null;
		}
	}

	private void checkAndCleanupSegment(Integer followerId, MappedWALFollower follower, int segmentId,
			int followerSegmentId) throws IOException {
		if (segmentId != followerSegmentId) {
			logger.info("Follower file(" + followerSegmentId + ") is doesn't match requested file id(" + segmentId
					+ "), reseting buffer & file id");
			follower.setBuf(null);
			if (follower.getReader() != null) {
				logger.finer("Follower(" + followerId + ") has existing open file, now closing");
				follower.getReader().close();
			}
			deleteWALSegments();
		}
	}

	private void updateMinOffset() {
		write.lock();
		long minimumOffset = getMinimumOffset();
		if (minimumOffset != Long.MAX_VALUE) {
			setCommitOffset(minimumOffset);
		}
		write.unlock();
	}

	private void checkAndRotateSegment(int size) throws IOException {
		if (currentWrite == null || currentWrite.remaining() < size) {
			// acquire a Write Lock
			write.lock();
			if (raf != null) {
				logger.fine("Flushing current write buffer");
				currentWrite.force();
				logger.fine("Closing write access to current segment file:"
						+ getSegmentFileName(walDirectory, segmentCounter));
				raf.close();
			}
			boolean forward = false;
			if (currentWrite == null) {
				segmentCounter = getLastSegmentCounter(walDirectory);
				if (segmentCounter > 1) {
					forward = true;
					logger.info("Existing segment file detected; segment counter set to:" + segmentCounter);
				}
			}
			raf = new RandomAccessFile(new File(getSegmentFileName(walDirectory, segmentCounter)), "rwd");
			currentWrite = raf.getChannel().map(MapMode.READ_WRITE, 0, segmentSize);
			logger.info("Creating new segment file:" + getSegmentFileName(walDirectory, segmentCounter));
			if (forward) {
				// if this segment file already exists then forward the cursor
				// to the
				// latest committed location;
				int pos = currentWrite.getInt(0);
				if (currentWrite.limit() > getCommitOffset()) {
					currentWrite.position(rawOffsetToSegmentOffset(getCommitOffset()));
				} else {
					currentWrite.position(pos);
				}
				logger.info("Found existing WAL:" + getSegmentFileName(walDirectory, segmentCounter)
						+ ", forwarding offset to:" + getCommitOffset());
			} else {
				// else write a blank 0 offset to the beginning of this brand
				// new buffer
				currentWrite.putInt(0);
			}
			logger.fine("Rotating segment file:" + getSegmentFileName(walDirectory, segmentCounter));
			segmentCounter++;
			write.unlock();
		}
	}

	// TODO note that this can cause issues if the segment is pre-maturely
	// terminated; need to run the math validation for whether or not this will work
	public int rawOffsetToSegmentOffset(long offset) {
		return (int) (offset % segmentSize);
	}

	public int rawOffsetToSegmentId(long offset) {
		return (int) (offset / segmentSize);
	}

	public static String getSegmentFileName(String walDirectory, int segmentCounter) {
		return walDirectory + "/" + String.format("%012d", segmentCounter) + ".wal";
	}

	public static int getLastSegmentCounter(String walDirectory) {
		File[] files = new File(walDirectory).listFiles(new FilenameFilter() {

			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(".wal");
			}
		});
		if (files.length > 0) {
			return Integer.parseInt(files[files.length - 1].getName().replace(".wal", ""));
		} else {
			return 0;
		}
	}

	@Override
	public int getSegmentCounter() {
		return segmentCounter;
	}

	@Override
	public long getCurrentOffset() {
		return currentWrite.position();
	}

	@Override
	public long getCommitOffset() {
		return commitOffset;
	}

	@Override
	public void setCommitOffset(long commitOffset) {
		write.lock();
		logger.finer("Updated commit offset:" + commitOffset);
		this.commitOffset = commitOffset;
		metaBuf.putLong(0, commitOffset);
		write.unlock();
	}

	@Override
	public long getFollowerOffset(Integer followerId) {
		MappedWALFollower mappedWALFollower = followerMap.get(followerId);
		if (mappedWALFollower != null) {
			return mappedWALFollower.getOffset();
		}
		return -1;
	}

	private long getMinimumOffset() {
		logger.finest("Getting minimum offset among followers");
		Integer[] array = followerMap.keySet().toArray(new Integer[0]);
		long offsetMarker = Long.MAX_VALUE;
		for (Integer tracker : array) {
			MappedWALFollower follower = followerMap.get(tracker);
			if (!follower.isIsr()) {
				logger.finest("Ignoring(" + tracker + ") since it's not an ISR offset:" + follower.getOffset()
						+ " current:" + currentWrite.position());
				continue;
			}
			long offset = follower.getOffset();
			if (offset < offsetMarker) {
				offsetMarker = offset;
			}
		}
		logger.finest("Commit offset:" + offsetMarker);
		return offsetMarker;
	}

	@Override
	public void setWALDeletion(boolean status) {
		walDeletion.set(status);
	}

	/**
	 * Delete WAL segments that are completely caught up
	 * 
	 * @throws InterruptedException
	 */
	private void deleteWALSegments() throws IOException {
		if (!walDeletion.get()) {
			return;
		}
		write.lock();
		int segmentMarker = Integer.MAX_VALUE;
		Integer[] array = followerMap.keySet().toArray(new Integer[0]);
		logger.fine("Follower Count:" + array.length);
		for (Integer tracker : array) {
			MappedWALFollower follower = followerMap.get(tracker);
			int fId = rawOffsetToSegmentId(follower.getOffset()) - 1;
			if (fId < segmentMarker) {
				segmentMarker = fId;
			}
		}
		logger.fine("Minimum segment to that can be deleted:" + segmentMarker);
		if (segmentMarker == segmentCounter || segmentMarker == Integer.MAX_VALUE) {
			logger.fine("Minimum segment is also the current segment, ignoring delete");
		} else {
			logger.fine("Segment compaction, will delete:" + segmentMarker + " files");
			for (int i = 0; i < segmentMarker; i++) {
				new File(getSegmentFileName(walDirectory, i)).delete();
				logger.info("Segment compaction, deleting file:" + getSegmentFileName(walDirectory, i));
			}
		}
		write.unlock();
	}

	@Override
	public Collection<Integer> getFollowers() {
		return followerMap.keySet();
	}

	@Override
	public boolean isIsr(Integer followerId) {
		return followerMap.get(followerId).isr;
	}

	private class MappedWALFollower {

		private volatile long offset;
		private RandomAccessFile reader;
		private MappedByteBuffer buf;
		private volatile boolean isr;

		/**
		 * @return the reader
		 */
		public RandomAccessFile getReader() {
			return reader;
		}

		/**
		 * @param reader
		 *            the reader to set
		 */
		public void setReader(RandomAccessFile reader) {
			this.reader = reader;
		}

		/**
		 * @return the buf
		 */
		public MappedByteBuffer getBuf() {
			return buf;
		}

		/**
		 * @param buf
		 *            the buf to set
		 */
		public void setBuf(MappedByteBuffer buf) {
			this.buf = buf;
		}

		/**
		 * @return the offset
		 */
		public long getOffset() {
			return offset;
		}

		/**
		 * @param offset
		 *            the offset to set
		 */
		public void setOffset(long offset) {
			this.offset = offset;
		}

		/**
		 * @return the isr
		 */
		public boolean isIsr() {
			return isr;
		}

		/**
		 * @param isr
		 *            the isr to set
		 */
		public void setIsr(boolean isr) {
			this.isr = isr;
		}

	}

}