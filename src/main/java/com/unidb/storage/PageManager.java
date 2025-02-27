package com.unidb.storage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PageManager {
    private static final int PAGE_SIZE = 4096; // 4KB per page
    private RandomAccessFile file;

    public PageManager(String filePath) throws IOException {
        File dbFile = new File(filePath);
        if (!dbFile.exists()) {
            dbFile.createNewFile();
        }
        this.file = new RandomAccessFile(dbFile, "rw");
    }

    // Read a page from the file
    public byte[] readPage(long pageId) throws IOException {
        byte[] buffer = new byte[PAGE_SIZE];
        file.seek(pageId * PAGE_SIZE);
        file.readFully(buffer);
        return buffer;
    }

    // Write a page to the file
    public void writePage(long pageId, byte[] data) throws IOException {
        if (data.length > PAGE_SIZE) {
            throw new IllegalArgumentException("Data exceeds page size");
        }
        file.seek(pageId * PAGE_SIZE);
        file.write(data);
    }

    // Allocate a new blank page and return its ID
    public long allocatePage() throws IOException {
        long newPageId = file.length() / PAGE_SIZE;
        file.seek(file.length());
        file.write(new byte[PAGE_SIZE]);
        return newPageId;
    }

    public void close() throws IOException {
        file.close();
    }

    public static void main(String[] args) {
        try {
            PageManager pm = new PageManager("unidb.db");
            long pageId = pm.allocatePage();
            log.info("Allocated Page ID: {}", pageId);

            byte[] data = "Hello UniDB!".getBytes();
            pm.writePage(pageId, data);
            byte[] readData = pm.readPage(pageId);

            log.info("Read Data: {}", new String(readData).trim());
            pm.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
