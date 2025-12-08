forked from [StoneForests/flink-mqtt-connector](https://github.com/StoneForests/flink-mqtt-connector  "StoneForests/flink-mqtt-connector")

[中文](README-zh.md "中文")

# flink-mqtt-connector

 Use flink 1.20.3 and [Eclipse Paho MQTT Java Client](https://github.com/eclipse/paho.mqtt.java "Eclipse Paho MQTT Java Client" ) 
 to realize a user-defined flinn mqtt api including table api and stream api. 
 It is able to read or write message via MQTT broker.
The usage of stream api can refer to MqttWordCount2MqttPaho.java, 
 while table api has to entries, one is for read-only in FlinkTableJustSource.java,
 the other is for read-write in FlinkTableSourceSink.java.
The principle  refers to https://blog.csdn.net/lck_csdn/article/details/125445017. 
 Thanks for the original authors.

# Changes

- Update to latest Flink 1.20.3
- Add format support(raw,csv,json)
- Combine source/sink topics together
- Support field mapping to rename field name
- Treat as anonymous when username and password are not provided
- Support embedded json

# How to use

## Compile and deploy

- Requiremetns
  
  ```bash
  $ java -version
  openjdk version "11.0.23" 2024-04-16
  OpenJDK Runtime Environment (build 11.0.23+9-post-Ubuntu-1ubuntu122.04.1)
  OpenJDK 64-Bit Server VM (build 11.0.23+9-post-Ubuntu-1ubuntu122.04.1, mixed mode, sharing)
  $ mvn -version
  Apache Maven 3.6.3
  Maven home: /usr/share/maven
  Java version: 11.0.23, vendor: Ubuntu, runtime: /usr/lib/jvm/java-11-openjdk-amd64
  Default locale: en_US, platform encoding: UTF-8
  OS name: "linux", version: "6.5.0-44-generic", arch: "amd64", family: "unix"
  ```
- compile

> mvn clean package

- deploy

    Under folder `target`, you will find generated package `flink-mqtt-connector-x.y.z.jar`. 
    Copy the `jar` file to `flink/lib/` and `pyfinkenv/lib/python3.10/site-packages/pyflink/lib/`(optional).

## Supported MQTT options

```python
'connector' = 'mqtt', # connector name
'hostUrl' = 'tcp://localhost:1883', # the mqtt's connect host url. string, no default value
'uername' = '',     # the mqtt's connect username. string, no default value
'password' = '',    # the mqtt's connect password. string, no default value
'topics' = '',  # the mqtt's sink topic. string, no default value
'clientIdPrefix' = '',   # the mqtt's connect client id's prefix. this is a logical application name. Pass a string, like “<<your-app-name>>”. string, default is randomUUID
'qos' = '1'   # the mqtt's sink qos. int, default is 1 ,  
'autoReconnect' = 'true',  # the mqtt's connect automatic reconnect.default is true
'cleanSession' = 'true', # the mqtt's source clean session. boolean, default is true
'connectionTimeout' = '30', # the mqtt's connect timeout. int,default is 30
'keepAliveInterval' = '60', # the mqtt's connect keep alive interval, int, default is 60
'sinkParallelism' = '1', # the mqtt's sink parallelism. int, default is 1.
'format' = 'raw', # refer to Flink document. support json,raw,csv.
'json.timestamp-format.standard' = 'ISO-8601', # timestamp format
'json.field.mapping' = 'tableFieldName:jsonFieldName' #support multiple mapping, use comma(,) as seperator 
```

## Examples

#### Flink SQL (sql-client.sh)

1. Source use `raw` format： OK
   
   ```SQL
   CREATE TABLE source(
   msg STRING
   ) WITH(
   'connector' = 'mqtt',
   'hostUrl' = 'tcp://localhost:1883',
   'username' = '',
   'password' = '',
   'topics' = 'test/mytopic',
   'format' = 'raw'
   );
   ```
   
   ```shell
              SQL Query Result (Table)
   Refresh: 1 s  Page: Last of 1 Updated: 14:09:44.125
                   msg
   ```

---------------------------

   {"id":3,"name":"ALLEN"}

```
Here use `mosquitto` as message broker, and use command `mosquitto_pub` to publish message manually.
```bash
$ mosquitto_pub -t test/mytopic -r -m {\"id\":3\,\"name\":\"ALLEN\"}
$ mosquitto_pub -t test/mytopic -r -m {\"id\":4\,\"name\":\"Jack\"}
```

2. Source use `json` foramt：OK
   
   ```SQL
   CREATE TABLE source(
     id INT,
     name STRING
   ) WITH(
   'connector' = 'mqtt',
   'hostUrl' = 'tcp://localhost:1883',
   'username' = '',
   'password' = '',
   'topics' = 'test/mytopic',
   'format' = 'json'
   );
   ```

```bash
$ mosquitto_pub -t test/mytopic -r -m {\"id\":3\,\"name\":\"ALLEN\"}
$ mosquitto_pub -t test/mytopic -r -m {\"id\":4\,\"name\":\"Jack\"}
```

3. Source use `csv`：OK
   
   ```SQL
   CREATE TABLE source(
     id INT,
     name STRING
   ) WITH(
   'connector' = 'mqtt',
   'hostUrl' = 'tcp://localhost:1883',
   'username' = '',
   'password' = '',
   'topics' = 'test/mytopic',
   'format' = 'csv'
   );
   ```
   
   ```bash
   $ mosquitto_pub -t test/mytopic -r -m 3\,\"ALLEN\"
   $ mosquitto_pub -t test/mytopic -r -m 4\,\"Jack\"
   ```

4. Sink use `raw` format:OK

__*Note：The 'raw' format only supports single physical column.*__

```SQL
CREATE TABLE sink (
     id_name STRING
 ) WITH (
  'connector' = 'mqtt',
  'hostUrl' = 'tcp://localhost:1883',
  'username' = '',
  'password' = '',
  'topics' = 'test/mytopic',
  'format' = 'raw'
 );

INSERT INTO sink (id_name) VALUES ('1,"Jack"');
```

5. Sink use`json` format：OK

```SQL
CREATE TABLE sink(
     id INT,
     name STRING
) WITH(
  'connector' = 'mqtt',
  'hostUrl' = 'tcp://localhost:1883',
  'username' = '',
  'password' = '',
  'topics' = 'test/mytopic',
  'format' = 'json'
 );

INSERT INTO sink (id,name) VALUES(1,'Jeen');
INSERT INTO sink (id,name) VALUES (2,'Jack');
```

6. Sink use`csv` format：OK

```SQL
CREATE TABLE sink(
     id INT,
     name STRING
) WITH(
  'connector' = 'mqtt',
  'hostUrl' = 'tcp://localhost:1883',
  'username' = '',
  'password' = '',
  'topics' = 'test/mytopic',
  'format' = 'csv'
 );

INSERT INTO sink (id,name) VALUES(1,'Jeen');
INSERT INTO sink (id,name) VALUES (2,'Jack');
```

### Use PyFlink Table API

```python
# filename: m66.py

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

env = StreamExecutionEnvironment.get_execution_environment()
settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
# create table environment
tab_env = StreamTableEnvironment.create(stream_execution_environment=env,environment_settings=settings)
# Add Kafka connector and dependencies
jar_dir = "/home/yin/flink-1.19.1/lib/"
jar_files=["flink-mqtt-connector-0.0.1.jar"]
jar_files = ";".join(["file://" + jar_dir+ x for x in jar_files])
print(jar_files)
tab_env.get_config().set("pipeline.jars",jar_files)

#######################################################################
# Create MQTT Source Table with DDL
#######################################################################
source_ddl = """
CREATE TABLE source(
 id INTEGER,
 name STRING
 ) WITH(
 'connector' = 'mqtt',
 'hostUrl' = 'tcp://localhost:1883',
 'username' = '',
 'password' = '',
 'topics' = 'test/mytopic',
 'format' = 'json'
 );
"""

sink_ddl = """
CREATE TABLE sink (
     json_result STRING
 ) WITH (
  'connector' = 'mqtt',
  'hostUrl' = 'tcp://localhost:1883',
  'username' = '',
  'password' = '',
  'topics' = 'test/mytopic',
  'format' = 'raw'
 );
"""

tab_env.execute_sql(source_ddl)
tab_env.execute_sql(sink_ddl)

query_sql = "INSERT INTO sink SELECT JSON_OBJECT('id' VALUE id+1, 'name' VALUE REVERSE(name)) FROM source"

tab_env.execute_sql(query_sql).wait()
```

### Use PyFlink Stream API

(to-do) Need a python wrapper 

### Note:

As an alternative method, we can use `JSON_OBJECT` function to write  `json` data in `raw` format.

```python
# filename: m66.py

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

env = StreamExecutionEnvironment.get_execution_environment()
settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
# create table environment
tab_env = StreamTableEnvironment.create(stream_execution_environment=env,environment_settings=settings)
# Add Kafka connector and dependencies
jar_dir = "/home/yin/flink-1.19.1/lib/"
jar_files=["flink-mqtt-connector-0.0.1.jar"]
jar_files = ";".join(["file://" + jar_dir+ x for x in jar_files])
print(jar_files)
tab_env.get_config().set("pipeline.jars",jar_files)

#######################################################################
# Create MQTT Source Table with DDL
#######################################################################
source_ddl = """
CREATE TABLE source(
 id INTEGER,
 name STRING
 ) WITH(
 'connector' = 'mqtt',
 'hostUrl' = 'tcp://localhost:1883',
 'username' = '',
 'password' = '',
 'topics' = 'test/mytopic',
 'format' = 'json'
 );
"""

sink_ddl = """
CREATE TABLE sink (
     json_result STRING
 ) WITH (
  'connector' = 'mqtt',
  'hostUrl' = 'tcp://localhost:1883',
  'username' = '',
  'password' = '',
  'topics' = 'test/mytopic',
  'format' = 'raw'
 );
"""

tab_env.execute_sql(source_ddl)
tab_env.execute_sql(sink_ddl)

query_sql = "INSERT INTO sink SELECT JSON_OBJECT('id' VALUE id+1, 'name' VALUE REVERSE(name)) FROM source"

tab_env.execute_sql(query_sql).wait()
```

The above example will read data `id,name` from topic `test/topic` in format `json`, 
then write back `id,name` after increasing `id+1` and reversing `name` to the same topic.
Therefore, it is an infinite loop. 

### how to run

#### run SQL script

> ~/flink-1.20.3/bin/sql-client -f mytest.sql

#### run python script

> $ ~/flink-1.19.1/bin/flink run --detached --python xx.py



# Known issues

- json.field.mapping
  
  - can not json field mapping when mutli topics have same field names.
  
  - logical field can not exist as NULL when physical field in json message doesn't exist if using json.field.mapping. In the other words, if not using json.field.mapping, the logical field will be set to NULL when relavant physical field doesn't exist in json message.
  
  - 

==  END  ==