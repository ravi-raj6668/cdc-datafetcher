package com.rasa.cdc.datafetcher.util;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

public class MSSQLBatchInserter {

    private static final String DB_URL = "jdbc:sqlserver://localhost:1433;databaseName=data_event;encrypt=false;trustServerCertificate=true";
    private static final String DB_USER = "sa";
    private static final String DB_PASSWORD = "root@123";
    private static final Random random = new Random();

    public static void main(String[] args) {
        Timer timer = new Timer();
        timer.schedule(new InsertTask(), 0, 10000); // every 10 seconds
    }

    static class InsertTask extends TimerTask {
        @Override
        public void run() {
            try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)) {
                conn.setAutoCommit(false);

                String sql = "INSERT INTO consumer_event (event_name, event_description, event_timestamp) VALUES (?, ?, ?)";
                try (PreparedStatement ps = conn.prepareStatement(sql)) {

                    for (int i = 0; i < 100; i++) {
                        String eventName = getRandomEventName();
                        String eventDescription = "User triggered " + eventName.toLowerCase() + " from " + getRandomSource();
                        LocalDateTime timestamp = LocalDateTime.now();

                        ps.setString(1, eventName);
                        ps.setString(2, eventDescription);
                        ps.setObject(3, timestamp);

                        ps.addBatch();
                    }

                    ps.executeBatch();
                    conn.commit();
                    System.out.println("✅ Inserted 100 rows at: " + LocalDateTime.now());
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private String getRandomEventName() {
            String[] names = {"UserLogin", "UserLogout", "DataUpdate", "AccountDelete", "FileUpload", "NotificationRead"};
            return names[random.nextInt(names.length)];
        }

        private String getRandomSource() {
            String[] sources = {"web", "mobile app", "API", "desktop"};
            return sources[random.nextInt(sources.length)];
        }
    }
}
