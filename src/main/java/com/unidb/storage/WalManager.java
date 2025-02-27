package com.unidb.storage;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class WalManager {
    private final File walFile;
    private final BufferedWriter writer;

    public WalManager(String filePath) throws IOException {
        this.walFile = new File(filePath);
        if (!walFile.exists()) {
            walFile.createNewFile();
        }
        this.writer = new BufferedWriter(new FileWriter(walFile, true));
    }

    // Append a log entry to WAL
    public void logWrite(long transactionId, long pageId, byte[] data) throws IOException {
        String logEntry = transactionId + "|" + pageId + "|" + new String(data) + "\n";
        writer.write(logEntry);
        writer.flush();
    }

    // Read all log entries from WAL
    public List<String> readLogs() throws IOException {
        List<String> logs = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(walFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                logs.add(line);
            }
        }
        return logs;
    }

    // Clear the WAL file (for checkpointing)
    public void clearWal() throws IOException {
        new FileWriter(walFile, false).close();
    }

    public void close() throws IOException {
        writer.close();
    }

    public static void main(String[] args) {
        try {
            WalManager walManager = new WalManager("unidb_wal.log");
            walManager.logWrite(1, 101, "Insert: Hello UniDB!".getBytes());
            walManager.logWrite(2, 102, "Update: World!".getBytes());

            List<String> logs = walManager.readLogs();
            log.info("WAL Logs:");
            logs.forEach(logString -> log.info(logString));

            walManager.clearWal();
            walManager.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}