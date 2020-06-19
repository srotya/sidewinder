package com.srotya.sidewinder.core.storage;

public interface Buffer {

	byte[] array();

	int capacity();

	Buffer duplicate(boolean readOnly);

	void flip();

	byte get();

	void get(byte[] buf);

	byte get(int position);

	int getInt();

	long getLong();

	long getLong(int position);

	int getShort();

	boolean hasRemaining();

	boolean isReadOnly();

	int limit();

	void limit(int position);

	Buffer newInstance(int capacity);

	int position();

	void position(int startOffset);

	void put(Buffer buf);

	void put(byte b);

	void put(byte[] b);

	void put(int offset, byte b);

	void putInt(int i);

	void putInt(int index, int value);
	
	void putLong(long xor);

	void putShort(short xor);

	int remaining();

	void rewind();

	Buffer slice();

}