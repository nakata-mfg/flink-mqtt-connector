package com.nakata.flink.connectors.mqtt.table;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import static com.nakata.flink.connectors.mqtt.table.MqttOptions.*;

public class MqttDynamicTableSink implements DynamicTableSink {
    private ReadableConfig mqttConfig;
    private ResolvedSchema physicalSchema;
    private EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat;

    public MqttDynamicTableSink(ReadableConfig mqttConfig,
                                EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat,
                                ResolvedSchema physicalSchema) {
        this.mqttConfig = mqttConfig;
        this.valueEncodingFormat =
                Preconditions.checkNotNull(
                        valueEncodingFormat, "Value encoding format must not be null.");
        this.physicalSchema = physicalSchema;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return valueEncodingFormat.getChangelogMode();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context runtimeProviderContext) {
        final SerializationSchema<RowData> serializer = valueEncodingFormat.createRuntimeEncoder(
                runtimeProviderContext,
                physicalSchema.toPhysicalRowDataType());
        String hostUrl = this.mqttConfig.get(HOST_URL);
        String username = this.mqttConfig.get(USERNAME);
        String password = this.mqttConfig.get(PASSWORD);
        String topics = this.mqttConfig.get(SINK_TOPICS);
        Integer qos = this.mqttConfig.get(QOS);
        String clientIdPrefix = this.mqttConfig.get(CLIENT_ID_PREFIX);
        Integer connectionTimeout = this.mqttConfig.get(CONNECTION_TIMEOUT);
        Integer keepAliveInterval = this.mqttConfig.get(KEEP_ALIVE_INTERVAL);
        boolean automaticReconnect = this.mqttConfig.get(AUTOMATIC_RECONNECT);
        final SinkFunction<RowData> sinkFunction = new MqttSinkFunction<>(
                hostUrl,
                username,
                password,
                topics,
                qos,
                clientIdPrefix,
                connectionTimeout,
                keepAliveInterval,
                automaticReconnect,
                serializer);

        Integer sinkParallelism = this.mqttConfig.get(SINK_PARALLELISM);
        return SinkFunctionProvider.of(sinkFunction,sinkParallelism);
    }

    @Override
    public DynamicTableSink copy() {
        return new MqttDynamicTableSink(mqttConfig, valueEncodingFormat, physicalSchema);
    }

    @Override
    public String asSummaryString() {
        return "Mqtt Table Sink";
    }
}
