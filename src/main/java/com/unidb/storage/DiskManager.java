package com.unidb.storage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DiskManager {
    private static final int PAGE_SIZE = 4096; // 4KB page size
    private final RandomAccessFile dbFile;
    private final BufferPoolManager bufferPool;

    public DiskManager(String filePath, int bufferPoolSize) throws IOException {
        File file = new File(filePath);
        if (!file.exists()) {
            file.createNewFile();
        }
        this.dbFile = new RandomAccessFile(file, "rw");
        this.bufferPool = new BufferPoolManager(bufferPoolSize);
    }

    // Read a page from disk with buffer pool support
    public byte[] readPage(long pageId) throws IOException {
        if (bufferPool.containsPage(pageId)) {
            return bufferPool.getPage(pageId);
        }
        byte[] buffer = new byte[PAGE_SIZE];
        dbFile.seek(pageId * PAGE_SIZE);
        dbFile.readFully(buffer);
        bufferPool.addPage(pageId, buffer);
        return buffer;
    }

    // Write a page to disk with buffer pool support
    public void writePage(long pageId, byte[] data) throws IOException {
        if (data.length > PAGE_SIZE) {
            throw new IllegalArgumentException("Data exceeds page size");
        }
        dbFile.seek(pageId * PAGE_SIZE);
        dbFile.write(data);
        bufferPool.addPage(pageId, data);
    }

    // Allocate a new blank page and return its ID
    public long allocatePage() throws IOException {
        long newPageId = dbFile.length() / PAGE_SIZE;
        dbFile.seek(dbFile.length());
        dbFile.write(new byte[PAGE_SIZE]);
        return newPageId;
    }

    public void close() throws IOException {
        dbFile.close();
    }

    public static void main(String[] args) {
        try {
            DiskManager dm = new DiskManager("unidb_storage.db", 10);
            long pageId = dm.allocatePage();
            log.info("Allocated Page ID: {}", pageId);
            
            byte[] data = "Hello UniDB!".getBytes();
            dm.writePage(pageId, data);
            byte[] readData = dm.readPage(pageId);
            
            log.info("Read Data: {}", new String(readData).trim());
            dm.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

