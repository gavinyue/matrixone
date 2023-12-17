MO Source and Dynamic Table User Doc
Overview
MO Source and Dyanmic Table provide tools to help efficiently perform analysis and stateful/stateless computations on streams of data. With MO Source, users can create, manage, and query data streams from various external sources, such as Kafka topics, and create continuous data pipelines to load, process, and persist the query results into the dynamic table for further analysis.
Use cases
Here are some illustrative applications:
	Identifying fraudulent activities and security breaches.
	Analyzing business and product metrics in real-time.
	Real-time data ingestion into MO database for in-depth analytics
	Generating machine learning features on-the-fly.
Architecture
[the architecture diagram/TBD]
1.	Need the overview of technical architeture.
2.	Need the picture of architeture.
3.	Need to explain the backgroud or scenarios
4.	Need to list some use case
      Quick start
      We need to show a simple example to explain how to use stream in a database.
      [Prepare a real business use case here, like user-click stream and product recommendation]
      Source
      Overview
      A source represents a external data stream source, such as Kafka topic.  After creating the Source, users can utilize MO's computing engine to perform in-depth query analysis.
      Syntax
      CREATE [OR REPLACE] SOURCE [IF NOT EXISTS] Source_A
      ( { column_name data_type [KEY | HEADERS | HEADER(key)] } [, ...] )
      WITH ( property_name = expression [, ...] Topic=A );

Properties
Required
Type
Specifies the stream underlying data source type.
Kafka Related
Topic
Authentication
Serde
SchemaRegistry
Examples
例一： 使用JSON， 不使用Schema_Registry
"value"= 'json',
"bootstrap.servers" = '127.0.0.1:9092',
"sasl.username" = '',
"sasl.password" = '',
"sasl.mechanisms" =  ''
"security.protocol" = ''
}
例二： 使用protobuf,
CREATE SOURCE user(
Id VARCHAR KEY // This the key of the message in Kafka
Name VARCHAR
Email VARCHAR
Phone VARCHAR )
WITH {
"type"='kafka',
"topic"= 'user',
"partion" = '1',
"value"= 'protobuf',
"protobuf.message" ='UserMessage'
"protobuf.schema" = '
syntax = "proto3";
option go_package = "./proto/test_v1";
package test_v1;

        message UserMessage {
              string Name = 1;
              string Email = 2;
              string Phone = 3;
    } '
    "bootstrap.servers" =  '127.0.0.1:62610',
    "sasl.username" = '',
    "sasl.password" = '',
    "sasl.mechanisms" =  ''
    "security.protocol" = ''
}

Data conversion
Failed data parsing
Dynamic Table
Overview
Description
Syntax
CREATE DYNAMIC TABLE [IF NOT EXISTS] table_name AS SELECT ... from stream_name WITH( property_name = expression [, ...]);
Properties
Latency
Messages Limit
Examples
create source testdb.test_stream (name varchar, age int) with ("type"='kafka', "topic"= 'test', "value"= 'json', "bootstrap.servers" = '127.0.0.1:9092');


create dynamic table testdb.testtable as select name from testdb.test_stream;


Failure Handling
Kafka Topic Message 某些情况完全丢弃，某些情况列置空？
Message Ordering
	多 CN 写 DT 表时，要保证顺序
产品已知行为和限制
	source 和 dynamic table 均不支持定义约束，例如 unique key
¡	将来要模仿 CTAS 语法
	当 kafka message 中的消息为空时，source 查询显示 null，而 dynamic table由于不能 insert null，所以没有这条信息
	全部脏数据：当 kafka message 中的消息与 source 定义的类型全部不匹配，则source 查询显示 null，而 dynamic table由于不能 insert null，所以没有这条信息
	部分脏数据：当 kafka message 中的消息与 source 定义的类型部分不匹配，则 source 和 dynamic table 都保留这条信息，不匹配的置为 null
¡	后续迭代可以做脏数据跳过
	dynamic table 在达到 time_window（默认 1000，单位 ms） 或 buffer_limit （ 默认 1）时会触发数据写入
	停止和继续 dynamic table 同步数据
	source 支持 select offset 吗？例如select....from...limit xx offset start [row|rows]

