package com.unidb.checkpointing;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import com.unidb.storage.WalManager;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CheckpointManager {
    private final WalManager walManager;
    private final String storagePath;

    public CheckpointManager(WalManager walManager, String storagePath) {
        this.walManager = walManager;
        this.storagePath = storagePath;
    }

    // Perform a checkpoint: flush WAL to permanent storage
    public void performCheckpoint() throws IOException {
        File storageFile = new File(storagePath);
        if (!storageFile.exists()) {
            storageFile.createNewFile();
        }

        try (BufferedWriter storageWriter = new BufferedWriter(new FileWriter(storageFile, true))) {
            List<String> logs = walManager.readLogs();
            for (String log : logs) {
                storageWriter.write(log + "\n");
            }
            storageWriter.flush();
        }
        walManager.clearWal();
        log.info("Checkpoint completed. WAL flushed to storage.");
    }

    public static void main(String[] args) {
        try {
            WalManager walManager = new WalManager("unidb_wal.log");
            CheckpointManager checkpointManager = new CheckpointManager(walManager, "unidb_storage.log");

            // Simulate WAL entries
            walManager.logWrite(1, 101, "Insert: Hello UniDB!".getBytes());
            walManager.logWrite(2, 102, "Update: World!".getBytes());

            // Perform checkpoint
            checkpointManager.performCheckpoint();

            walManager.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
