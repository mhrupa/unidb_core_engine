package com.unidb.storage;

import java.util.LinkedHashMap;
import java.util.Map;

public class BufferPoolManager {
    private final int capacity;
    private final Map<Long, byte[]> bufferPool;

    public BufferPoolManager(int capacity) {
        this.capacity = capacity;
        this.bufferPool = new LinkedHashMap<>(capacity, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Long, byte[]> eldest) {
                return size() > BufferPoolManager.this.capacity;
            }
        };
    }

    public boolean containsPage(long pageId) {
        return bufferPool.containsKey(pageId);
    }

    public byte[] getPage(long pageId) {
        return bufferPool.get(pageId);
    }

    public void addPage(long pageId, byte[] data) {
        bufferPool.put(pageId, data);
    }
}