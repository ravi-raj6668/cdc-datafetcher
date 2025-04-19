package com.rasa.cdc.datafetcher.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rasa.cdc.datafetcher.entity.EventDTO;
import io.debezium.config.Configuration;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class DebeziumCDCListener {

    @Autowired
    private KafkaProducerService producerService;

    @PostConstruct
    public void startDebeziumEngine() {
        Configuration config = Configuration.create()
                .with("name", "mssql-engine")
                .with("connector.class", "io.debezium.connector.sqlserver.SqlServerConnector")

                // Offset config - stored locally
                .with("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
                .with("offset.storage.file.filename", "offsets-mssql.dat")
                .with("offset.flush.interval.ms", "60000")

                // Schema history config - stored locally (no Kafka)
                .with("schema.history.internal", "io.debezium.storage.file.history.FileSchemaHistory")
                .with("schema.history.internal.file.filename", "schema-history-mssql.dat")

                // SQL Server database config
                .with("database.hostname", "localhost")
                .with("database.port", "1433")
                .with("database.user", "sa")
                .with("database.password", "root@123")
                .with("database.names", "data_event")
                .with("table.include.list", "dbo.consumer_event")
                .with("database.encrypt", "false")
                .with("database.trustServerCertificate", "true")

                // Required topic prefix (used for internal naming only, not Kafka topics)
                .with("topic.prefix", "sqlserver")

                .build();

        DebeziumEngine<ChangeEvent<String, String>> engine = DebeziumEngine.create(Json.class)
                .using(config.asProperties())
                .notifying(this::handleChangeEvent)
                .build();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(engine);
    }

    private void handleChangeEvent(ChangeEvent<String, String> event) {
        try {
            if (event.value() == null) return;

            ObjectMapper mapper = new ObjectMapper();
            JsonNode rootNode = mapper.readTree(event.value());
            JsonNode payload = rootNode.get("payload");

            if (payload == null || payload.isNull()) return;

            JsonNode after = payload.get("after");
            if (after == null || after.isNull()) return;

            EventDTO eventDTO = new EventDTO();
            eventDTO.setId(after.get("id").asInt());
            eventDTO.setEventName(after.get("event_name").asText());
            eventDTO.setEventDescription(after.get("event_description").asText());

            String timestampStr = after.get("event_timestamp").asText();
            LocalDateTime timestamp;

            // Check if the timestamp is numeric (epoch millis)
            if (timestampStr.matches("\\d+")) {
                long epochMillis = Long.parseLong(timestampStr);
                timestamp = java.time.Instant.ofEpochMilli(epochMillis)
                        .atZone(java.time.ZoneId.systemDefault())
                        .toLocalDateTime();
            } else {
                // Remove 'Z' or offset if present
                if (timestampStr.endsWith("Z")) {
                    timestampStr = timestampStr.replace("Z", "");
                } else if (timestampStr.contains("+")) {
                    timestampStr = timestampStr.substring(0, timestampStr.indexOf("+"));
                }
                timestamp = LocalDateTime.parse(timestampStr, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            }

            eventDTO.setEventTimestamp(String.valueOf(timestamp));
            producerService.publishProcessedMsg(eventDTO);
            System.out.println("New event inserted: " + mapper.writeValueAsString(eventDTO));
        } catch (Exception e) {
            System.err.println("Error processing change event: " + e.getMessage());
            e.printStackTrace();
        }
    }

}
