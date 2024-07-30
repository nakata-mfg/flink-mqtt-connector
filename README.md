# flink-mqtt-connector
 使用flink1.14.3和paho mqtt客户端实现的自定义flink mqtt connector，分别使用table api和stream api进行了实现，可以从mqtt执行读取数据，写入数据。
 stream api的入口在MqttWordCount2MqttPaho.java，table api有两个入口，其中只读mqtt的入口是FlinkTableJustSource.java，又读又写mqtt的是FlinkTableSourceSink.java。
 原理见https://blog.csdn.net/lck_csdn/article/details/125445017， 感谢原文作者！

# 变更
- 更新依赖至最新版本（兼容Flink 1.19.1）
- 增加格式支持 (raw,csv,json)

# 使用说明
## 支持的MQTT选项
```python
'connector' = 'mqtt', # 指定工厂类的标识符，该标识符就是建表时必须填写的connector参数的值
'hostUrl' = 'tcp://localhost:1883', # the mqtt's connect host url. string, no default value
'uername' = '',     # the mqtt's connect username. string, no default value
'password' = '',    # the mqtt's connect password. string, no default value
'sinkTopics' = '',  # the mqtt's sink topic. string, no default value
'sourceTopics' = '',  # the mqtt's source topics. no default value
'clientIdPrefix' = '',   # the mqtt's connect client id's prefix. this is a logical application name. Pass a string, like “<<your-app-name>>”. string, default is randomUUID
'qos' = '1'   # the mqtt's sink qos. int, default is 1 ,  
'autoReconnect' = 'true',  # the mqtt's connect automatic reconnect.default is true
'cleanSession' = 'true', # the mqtt's source clean session. boolean, default is true
'connectionTimeout' = '30', # the mqtt's connect timeout. int,default is 30
'keepAliveInterval' = '60', # the mqtt's connect keep alive interval, int, default is 60
'sinkParallelism' = '1', # the mqtt's sink parallelism. int, default is 1.
'format' = 'raw', # 数据格式，参照Flink文档。 
```

## 使用例子
#### Flink SQL (sql-client.sh)
1. Source采用raw格式： OK
```SQL
CREATE TABLE source(
 msg STRING
 ) WITH(
 'connector' = 'mqtt',
 'hostUrl' = 'tcp://localhost:1883',
 'username' = '',
 'password' = '',
 'sourceTopics' = 'test/mytopic',
 'format' = 'raw'
 );
```
```shell
              SQL Query Result (Table)
Refresh: 1 s  Page: Last of 1 Updated: 14:09:44.125
                   msg
---------------------------
   {"id":3,"name":"ALLEN"}
```

采用mosquitto作为消息broker，用mosquitto_pub手动更新消息
```bash
$ mosquitto_pub -t test/mytopic -r -m {\"id\":3\,\"name\":\"ALLEN\"}
$ mosquitto_pub -t test/mytopic -r -m {\"id\":4\,\"name\":\"Jack\"}
```

2. Source采`json`格式：OK
```SQL
CREATE TABLE source(
     id INT,
     name STRING
) WITH(
  'connector' = 'mqtt',
  'hostUrl' = 'tcp://localhost:1883',
  'username' = '',
  'password' = '',
  'sourceTopics' = 'test/mytopic',
  'format' = 'json'
 );
 ```

```bash
$ mosquitto_pub -t test/mytopic -r -m {\"id\":3\,\"name\":\"ALLEN\"}
$ mosquitto_pub -t test/mytopic -r -m {\"id\":4\,\"name\":\"Jack\"}
```

3. Source采用csv格式：OK
```SQL
CREATE TABLE source(
     id INT,
     name STRING
) WITH(
  'connector' = 'mqtt',
  'hostUrl' = 'tcp://localhost:1883',
  'username' = '',
  'password' = '',
  'sourceTopics' = 'test/mytopic',
  'format' = 'csv'
 );
 ```
```bash
$ mosquitto_pub -t test/mytopic -r -m 3\,\"ALLEN\"
$ mosquitto_pub -t test/mytopic -r -m 4\,\"Jack\"
```


4. Sink采用`raw`格式:OK

__*注意：The 'raw' format only supports single physical column.*__

```SQL
CREATE TABLE sink (
     id_name STRING
 ) WITH (
  'connector' = 'mqtt',
  'hostUrl' = 'tcp://localhost:1883',
  'username' = '',
  'password' = '',
  'sinkTopics' = 'test/mytopic',
  'format' = 'raw'
 );

INSERT INTO sink (id_name) VALUES ('1,"Jack"');
```

5. Sink采用`json`格式：NG 

```SQL
CREATE TABLE sink(
     id INT,
     name STRING
) WITH(
  'connector' = 'mqtt',
  'hostUrl' = 'tcp://localhost:1883',
  'username' = '',
  'password' = '',
  'sinkTopics' = 'test/mytopic',
  'format' = 'json'
 );

INSERT INTO sink (id,name) VALUES(1,'Jeen');
INSERT INTO sink (id,name) VALUES (1,'Jack');
```
6. Sink采用`csv`格式：NG

```SQL
CREATE TABLE sink(
     id INT,
     name STRING
) WITH(
  'connector' = 'mqtt',
  'hostUrl' = 'tcp://localhost:1883',
  'username' = '',
  'password' = '',
  'sinkTopics' = 'test/mytopic',
  'format' = 'csv'
 );

INSERT INTO sink (id,name) VALUES ('1,"Jeen"');
INSERT INTO sink (id,name) VALUES (''1,"Jack"');
```

### 使用PyFlink Table API

### 使用PyFlink Stream API
