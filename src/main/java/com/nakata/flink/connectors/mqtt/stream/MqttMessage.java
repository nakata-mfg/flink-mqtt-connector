package com.nakata.flink.connectors.mqtt.stream;


import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.types.RowKind;

import java.io.Serializable;
import java.util.Collections;

import static org.apache.flink.util.Preconditions.checkNotNull;

import com.nakata.flink.connectors.mqtt.JsonConverter;

@Data
@Slf4j
public class MqttMessage implements Serializable {

    private static final long serialVersionUID = -4673414704450588069L;

    private String topic;
    private Integer qos;
    private byte[] payload;
    private RowKind kind;
    private final Object[] fields;

//    public MqttMessage() {
//    }

    public MqttMessage(String topic, Integer qos, byte[] payload, RowKind kind) {
        Object[] msgFields;
        this.topic = topic;
        this.qos = qos;
        this.payload = payload;
        if (this.payload != null) {
            try {
                msgFields = JsonConverter.deserialize(payload).values().toArray();
            } catch (Exception e) {
                log.warn("data is not a valid json:" + new String(payload));
                msgFields = Collections.emptyList().toArray();
            }
        } else {
            msgFields = Collections.emptyList().toArray();
        }
        fields = msgFields;
        checkNotNull(kind);
        this.kind = kind;
    }


}
