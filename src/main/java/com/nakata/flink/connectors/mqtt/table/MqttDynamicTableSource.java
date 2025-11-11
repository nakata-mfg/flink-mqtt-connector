package com.nakata.flink.connectors.mqtt.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import java.util.HashMap;
import java.util.Map;

import static com.nakata.flink.connectors.mqtt.table.MqttOptions.*;

public class MqttDynamicTableSource implements ScanTableSource {
    private ReadableConfig options;
    private ResolvedSchema schema;
    private DecodingFormat<DeserializationSchema<RowData>> decodingFormat;

    public MqttDynamicTableSource(ReadableConfig options, DecodingFormat<DeserializationSchema<RowData>> decodingFormat, ResolvedSchema schema) {
        this.options = options;
        this.decodingFormat = decodingFormat;
        this.schema = schema;
    }

    @Override
    //写入方式默认INSERT_ONLY,里面实现了一个static静态类初始化
    public ChangelogMode getChangelogMode() {
//        return decodingFormat.getChangelogMode();
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
//                .addContainedKind(RowKind.UPDATE_BEFORE)
//                .addContainedKind(RowKind.UPDATE_AFTER)
//                .addContainedKind(RowKind.DELETE)
                .build();
    }

    @Override
    //获取运行时类
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext ctx) {

        // 创建运行时类用于提交给集群
        final DeserializationSchema<RowData> deserializer = decodingFormat.createRuntimeDecoder(
                ctx,
                schema.toPhysicalRowDataType());

        final String mappingStr = options.get(MqttOptions.JSON_FIELD_MAPPING);
        final Map<String, String> jsonFieldMapping = new HashMap<>();
        if (mappingStr != null && !mappingStr.isEmpty()) {
            for (String entry : mappingStr.split(",")) {
                String[] kv = entry.split(":");
                if (kv.length == 2) {
                    jsonFieldMapping.put(kv[0].trim(), kv[1].trim());
                }
            }
        }

        String broker = this.options.get(HOST_URL);
        String username = this.options.get(USERNAME);
        String password = this.options.get(PASSWORD);
        String topics = this.options.get(TOPICS);
        boolean cleanSession = this.options.get(CLEAN_SESSION);
        String clientIdPrefix = this.options.get(CLIENT_ID_PREFIX);
        Integer connectionTimeout = this.options.get(CONNECTION_TIMEOUT);
        Integer keepAliveInterval = this.options.get(KEEP_ALIVE_INTERVAL);
        boolean automaticReconnect = this.options.get(AUTOMATIC_RECONNECT);
        Integer maxInflight = this.options.get(MAX_INFLIGHT);
        Long pollInterval = this.options.get(POLL_INTERVAL);
        final SourceFunction<RowData> sourceFunction = new MqttSourceFunction<>(broker, username, password, topics, cleanSession, clientIdPrefix, automaticReconnect, connectionTimeout, keepAliveInterval, maxInflight, pollInterval, deserializer,jsonFieldMapping);
        return SourceFunctionProvider.of(sourceFunction, false);
    }

    @Override
    public DynamicTableSource copy() {
        return new MqttDynamicTableSource(options, decodingFormat, schema);
    }

    @Override
    public String asSummaryString() {
        return "Mqtt Table Source";
    }
}
