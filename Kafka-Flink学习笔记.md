# Kafka-Flink学习笔记

## 一.Kafka

### 1.概念

- Kafka是一种高吞吐量的分布式发布订阅消息系统。

- Kafka集群包含一个或多个服务器，这种代理服务器被称为Broker。

- 每条发布到Kafka集群的消息都有一个类别，这个类别被称为Topic，功能类似于数据库表，存储数据。

- replication-factor（复制因子），定义备份数的参数，功能类似hdfs备份

- Partition（分区），定义分区数的参数，功能类似表分区

- Producer（生产者），负责生产消息，即可以从其他数据源或数据流抽取数据存入kafka消息队列

- Consumer（消费者），负责消费kafka消息队列中的数据，在抽取的过程中可以进行一系列的逻辑与类型转换

  注：生产者与消费者都可以与其他的组件相结合，如

  [flink]: https://ci.apache.org/projects/flink/flink-docs-release-1.11/zh/dev/connectors/filesystem_sink.html
  [spark]: http://spark.apache.org/docs/latest/streaming-programming-guide.html

  详细请看官网

### 2.特性

- 通过O(1)的磁盘数据结构提供消息的持久化，这种结构对于即使数以TB的消息存储也能够保持长时间的稳定性能。

- 高吞吐量：即使是非常普通的硬件Kafka也可以支持每秒数百万的消息。

- 支持通过Kafka服务器和消费机集群来分区消息。

- 支持[Hadoop](https://baike.baidu.com/item/Hadoop)并行数据加载。

  注：o(1) 说的是复杂度，复杂度包括时间复杂度和空间复杂度，这里应该是时间复杂度，表示在取消息时就像数组一样，根据下标直接就获取到了

  消费机集群指的是消费者所处节点组成的集群

### 3.生产者

#### ①.参数配置

- ```
  bootstrap.servers：指定kafka的地址与端口，如192.168.22.1:9092。
  ```

- ```
  key.serializer:指定键序列化的程序类->org.apache.kafka.common.serialization.Serializer。
  ```

- ```
  value.serializer:指定值序列化的程序列->org.apache.kafka.common.serialization.Serializer。
  ```

- ```
  acks:在请求完成之前，生产者要求领导者已收到的确认数。这控制发送的记录的持久性。
  - 0表示生产者发送了就表示发送成功，即不等待leader返回的确认，不能保证leader成功保存记录。
  - 1表示leader会将记录写入本地日志，但不会等待所有追随者的完全确认，在这种情况下，如果领导者在确认记录后，但在跟随者复制之前立即失败，则记录将丢失。
  - all等同于-1表示leader保证只要至少有一个同步副本保持活动状态，记录就不会丢失
  ```

- ```
  retries:允许请求失败重试次数
  ```

- ```
  delivery.timeout.ms:指定传输超时时间>延迟时间（linger.ms）+ 请求响应时间（request.timeout.ms）
  ```

- ```
  request.timeout.ms:指定请求响应超时时间
  ```

- ```:
  linger.ms:延迟时间，将时间范围内的请求整理为一个批处理请求，减少请求数，默认1
  ```

- ```
  batch.size:单位byte 将多个请求整合成一个批处理请求，和linger.ms配合使用，若大于batch.size，则直接发送请求，默认16384
  ```

- ```
  buffer.memory:生产者缓存字节数，默认33554432
  ```

- ```
  compression.type:压缩方式，none,gzip,snappy,lz4,zstd
  ```

- ```
  enable.idempotence:实现幂等性，消息自动去重，若为实现事务该参数必为true，会默认把acks设置为all
  ```

- ```
  transactional.id:指定事务ID，开启事务必须指定
  ```

- ```
  replication.factor:复制因子，指定备份数
  ```

- ```
  min.insync.replicas:当acks指定为“all”或-1时，min.insync.replicas必须指定一个数值，表示最少有几个副本成功写入，才能认为此次写入成功，否则将报错
  ```

- ```
  max.in.flight.requests.per.connection：限制客户端在单个连接上能够发送的未响应请求的个数
  ```

- ```
  unclean.leader.election.enable：指定不在isr（In-Sync Replicas）集中的副本是否可以参加leader的选举，默认为false（不可参加）。
  - false会降低kafka的可用性，可能导致任务失败挂掉
  - true会降低kafka 的可靠性，可能导致数据丢失或数据不一致
  ```

- ```
  max.block.ms：指定阻塞超时的时长，若批次数据量较大，应调高此参数
  ```

- ```
  buffer.memory：缓存等待发送到服务端的数据
  ```

- ```
  transaction.timeout.ms：两次提交允许的间隔时间
  ```

  注：更多参数配置请查看官网自行配置

  [kafka官方文档]: http://kafka.apache.org/documentation/#producerconfigs

#### ②.开启事务（幂等性）

- 目的：产者进行retry会产生重试时，会重复产生消息，开启之后，在进行retry重试时，只会生成一个消息。

- PID：每个新的Producer在初始化的时候会被分配一个唯一的PID，这个PID对用户是不可见的。

- Sequence Numbler：对于每个PID，该Producer发送数据的每个<Topic, Partition>都对应一个从0开始单调递增的Sequence Number。

- 开启流程：

  ①.设置enable.idempotence配置为true；

  ②.配置transaction.id属性；

  ③.调用API启用事务。

- 相关API：

  ①.initTransactions()初始化事务；

  ②.beginTransaction()开启事务；

  ③.commitTransaction()提交事务；

  ④.abortTransaction()放弃事务，类似回滚事务的操作;
  
  ⑤.sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,String consumerGroupId)为消费者提供的在事务内提交偏移量的操作。

### 4.消费者

#### ①.参数配置

- ```
  bootstrap.servers：指定kafka的地址与端口，如192.168.22.1:9092。
  ```

- ```
  key.deserializer：指定键反序列化的程序类->org.apache.kafka.common.serialization.StringDeserializer。
  ```

- ```
  value.deserializer：指定值反序列化的程序类->org.apache.kafka.common.serialization.StringDeserializer。
  ```

- ```
  group.id：指定消费者的归属组id。
  ```

- ```
  enable.auto.commit：选择false后关闭后台自动提交offset。
  ```

- ```
  fetch.min.bytes：当服务端累积数据大于配置值或提取时间超时，则提取数据，提高延迟增大吞吐量
  ```

- ```
  request.timeout.ms：指定请求超时时间
  ```

- ```
  session.timeout.ms：指定session超时时间
  ```

- ```
  auto.offset.reset：指定访问的开始位置，若没有offset，则从最早数据开始访问,[latest, earliest, none]
  ```

- isolation.level：指定消费的级别，配置read_committed后只消费成功提交的事务消息，非事务消息任何级别都可以消费

注：更多参数配置请查看官网自行配置

[kafka官方文档]: http://kafka.apache.org/documentation/#producerconfigs

## 二.Flink

### 1.概念

- Flink的核心是用Java和Scala编写的分布式流数据流引擎。
- Flink以数据并行和流水线方式执行任意流数据程序，Flink的流水线运行时系统可以执行批处理和流处理程序。
- Flink流处理可以通过窗口来对流数据进行截断聚合统计操作，通过水位线来保证数据排序正确，通过检查点（快照）来进行安全保障，防止数据丢失或数据重复。

### 2.时间特征（TimeCharacteristic）

- ProcessingTime是以operator处理的时间为准，它使用的是机器的系统时间来作为data stream的时间
- IngestionTime是以数据进入flink streaming data flow的时间为准
- EventTime是以数据自带的时间戳字段为准，应用程序需要指定如何从record中抽取时间戳字段

### 3.检查点（CheckPoint）

- 类似于快照，通过StreamExecutionEnvironment对象的enableCheckpointing(5000)方法来开启检查点，5000表示每个5S设定一个检查点，即按一分钟切点，每5s一个检查点，当在(0,5)报错时，重试会返回到0s的状态重新开始

### 4.窗口类型（Window）

- Tumbling window（翻滚窗口）：分成两种类型，一个是时间翻滚窗口timeWindow(Time size),一个是计数翻滚窗口countWindow(long side)，时间翻滚窗口根据参数将每分钟依次切片，计数翻滚窗口将数据流按参数来截取。
- sliding window（滑动窗口）：分成两种类型，一个是时间滑动窗口timeWindow(Time size,Time slide),一个是计数滑动窗口countWindow(long side,long slide)，滑动窗口通过第一个参数来限定窗口大小，通过第二个参数来表示滑动长度，如timeWindow(5s,2s)，假设当前窗口在【0，5】，当过了2s窗口范围将变为【2，7】。
- session window（会话窗口）：这种窗口主要是根据活动的事件进行窗口化，他们通常不重叠，也没有一个固定的开始和结束时间。一个session window关闭通常是由于一段时间没有收到元素。
- global window（全局窗口）：将所有相同keyed的元素分配到一个窗口里。
- custom window（自定义窗口）

### 5.水位线（WaterMark）

- 目的：WaterMark是为了处理乱序事件，在数据传输中，由于各种原因，如传输、逻辑操作等，中间是有一个过程和时间的。虽然大部分情况下，流到operator的数据都是按照事件产生的时间顺序来的，但是也不排除由于网络、背压等原因，导致乱序的产生。
- 措施：在各个窗口中，它会通过读取数据的时间，可能是操作时间或事件时间，具体看重写的方法逻辑，来获取一个最大的时间戳成为该窗口的WaterMark，通过判定该窗口和往后一段时间内其他窗口的数据时间是否小于该窗口的WaterMark，来保证原属于该窗口中的所有的数据能成功到达。

### 三.案例解析

- StreamEnvironment类通过定义一个全局的StreamExecutionEnvironment对象，然后通过getEnvironment()来调用获取，以此来避免生成过多的StreamExecutionEnvironment对象。

```java
package com.xmaviation.environment;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.flink.streaming.api.CheckpointingMode.EXACTLY_ONCE;

public class StreamEnvironment {
    private static StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();;
    public static StreamExecutionEnvironment getEnvironment(){
        see.enableCheckpointing(5000); //开启检查点，检查点间隔为5000ms
        see.setParallelism(3);//设置全局各操作的默认并行度，优先级低于操作自主设置的值
        see.getCheckpointConfig().setCheckpointingMode(EXACTLY_ONCE);//设置检查点的模式
        see.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);//选择StreamTime的特性
        return see;
    }
}
```

- KafkaProducerFactory类通过定义一个Properties对象并对其进行属性配置，然后Properties对象通过参数赋值生成一个KafkaProducer对象，最后通过return返回KafkaProducer对象，通过调用该类，输入不同的kafkaAddress与transactionalId，直接返回一个新的KafkaProducer对象。

```java
package com.xmaviation.factory;

import org.apache.kafka.clients.producer.KafkaProducer;
import java.util.Properties;

public class KafkaProducerFactory {
    public static KafkaProducer getKafkaProducer(String kafkaAddress,int transactionalId) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers",kafkaAddress);
        properties.put("acks","all");
        properties.put("retries",5); //允许请求失败重试次数
        properties.put("delivery.timeout.ms",300000); //传输时间 300s >延迟时间（linger.ms）+ 请求响应时间（request.timeout.ms）
        properties.put("request.timeout.ms",60000);
        properties.put("linger.ms",1); //延迟时间，将时间范围内的请求整理为一个批处理请求，减少请求数
        properties.put("batch.size",16384); //单位byte 将多个请求整合成一个批处理请求，和linger.ms配合使用，若大于batch.size，则直接发送请求
        properties.put("buffer.memory", 33554432); //生产者缓存字节数
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("compression.type","none"); //压缩方式，none,gzip,snappy,lz4,zstd
        properties.put("enable.idempotence",true); //实现幂等性，消息自动去重，若为实现事务该参数必为true
        properties.put("transactional.id","TransactionalId-"+ transactionalId); //事务ID
        properties.put("replication.factor",1);
        properties.put("min.insync.replicas",1);
        properties.put("max.in.flight.requests.per.connection",1); //限制客户端在单个连接上能够发送的未响应请求的个数
        properties.put("unclean.leader.election.enable",false);
        properties.put("max.block.ms",180000); //阻塞时长
        properties.put("buffer.memory",33554432); //缓存等待发送到服务端的数据
        properties.put("transaction.timeout.ms",60000); //两次提交允许的间隔
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String,String>(properties);
        return  kafkaProducer;
    }
}
```

- ①.ProduceThreadFactory类以Runnable为接口，通过先声明一个KafkaProducer对象，然后通过构造函数来获取调用KafkaProducerFactory类的getKafkaProducer(）方法给KafkaProducer赋值，重写run()方法；

  ②.通过从文件中读取数据，并通过bufferedReader.readLine()方法按行将其发送到kafka消息队列中和事务相关的API来开启事务。

```java
package com.xmaviation.factory;

import com.xmaviation.utils.DateFormatUtil;
import lombok.SneakyThrows;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.*;
import java.io.*;
import java.util.Date;

public class ProduceThreadFactory implements Runnable {
    private long count = 0;
    private String source, topic;
    private KafkaProducer<String, String> kafkaProducer;
    private String message = null;

    public ProduceThreadFactory(String source, String topic, int transactionalId) {
        this.source = source;
        this.topic = topic;
        kafkaProducer = KafkaProducerFactory.getKafkaProducer("***.***.**.***:9092", transactionalId);
    }

    @SneakyThrows
    @Override
    public void run() {
        kafkaProducer.initTransactions();//初始化事务
        BufferedReader bufferedReader = new BufferedReader(
                new FileReader(
                        new File(source)
                )
        );
        while ((message = bufferedReader.readLine()) != null) {
            if (count % 30000 == 0) {
                kafkaProducer.beginTransaction();//开启事务
                System.out.println(count + "_" + DateFormatUtil.simpleDateFormat1.format(new Date()) + "_beginTransaction");
            }
            kafkaProducer.send(new ProducerRecord<>(topic, message), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null) {
                        kafkaProducer.abortTransaction();//放弃事务
                        System.out.println("Faild to send message because " + e);
                    }
                }
            });
            if (++count % 30000 == 0) {
                kafkaProducer.commitTransaction();//提交事务
                System.out.println(count + "_" + DateFormatUtil.simpleDateFormat1.format(new Date()) + "_commitTransaction");
            }
        }
        kafkaProducer.commitTransaction();
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
```

- KafkaConsumerFactory类通过定义一个Properties对象并对其进行属性配置，然后Properties对象通过参数赋值生成一个FlinkKafkaConsumer010对象，最后通过return返回FlinkKafkaConsumer010对象，通过调用该类，输入不同的bootstrapServer、topic与groupId，直接返回一个新的FlinkKafkaConsumer010对象。

```java
package com.xmaviation.factory;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.util.Properties;

public class KafkaConsumerFactory {
    public static FlinkKafkaConsumer010 getKafkaConsumer(String bootstrapServer,String topic,int groupId) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers",bootstrapServer);
        properties.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "groupId-" + groupId);
        properties.put("enable.auto.commit", "false"); //关闭后台自动提交offset
        properties.put("fetch.min.bytes",1);   //当服务端累积数据大于配置值或提取超时，则提取数据，提高延迟增大吞吐量
        properties.put("request.timeout.ms",60000); //请求时间
        properties.put("session.timeout.ms",180000); //会话时间
        properties.put("auto.offset.reset","earliest"); //若没有offset，则从最早数据开始访问,[latest, earliest, none]
        properties.put("isolation.level","read_committed"); //只消费成功提交的事务消息，非事务都可以消费
//        properties.put("fs.default-scheme","hdfs://***.***.**.***:8020");//读取hadoop平台上的相关配置，如hdfs-site.xml等
        FlinkKafkaConsumer010 flinkKafkaConsumer010 = new FlinkKafkaConsumer010(topic,new SimpleStringSchema(),properties);
        return flinkKafkaConsumer010;
    }
}
```

- MessageSplitterUtils类重写了flatMap，通过判断value是否为空和是否包含","来对其字段切分

```java
package com.xmaviation.utils;

import com.xmaviation.entity.AirportRiskEntity;
import com.xmaviation.helper.JSONHelper;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @Description    MessageSplitter 将获取到的每条Kafka消息根据“,”分割
 * @Author         YiCong Yang
 * @CreateDate     2020/06/11
 */
public class MessageSplitterUtils implements FlatMapFunction<String, Tuple2<String, Long>> {
    @Override
    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
        if (value != null && value.contains(",")) {
            String[] parts = value.split(",");
            out.collect(new Tuple2<>(parts[0], 1L));
        }
    }
}
```

- MessageWaterEmitterUtils类重写checkAndGetNextWatermark()和extractTimestamp()方法，该处是通过extractTimestamp方法获取当前时间，并通过Math.max()函数将最大时间保留到waterMarkTimeStamp，然后通过checkAndGetNextWatermark方法返回waterMarkTimeStamp，得到该窗口的水位线

```java
package com.xmaviation.utils;

import lombok.SneakyThrows;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Description MessageWaterEmitter 根据当前时间戳确定Flink的水位线
 * @Author YiCong Yang
 * @CreateDate 2020/06/11
 */
public class MessageWaterEmitterUtils implements AssignerWithPunctuatedWatermarks<String> {
    private long waterMarkTimeStamp,previousElementTimestamp;
        @SneakyThrows
    	@Override
        public Watermark checkAndGetNextWatermark(String lastElement, long extractedTimestamp) {
        if (lastElement != null) {
            return new Watermark(waterMarkTimeStamp);
        }
        return null;
    }

    @SneakyThrows
    @Override
    public long extractTimestamp(String element, long previousElementTimestamp) {
        if (element != null){
            previousElementTimestamp = Long.parseLong(DateFormatUtil.simpleDateFormat.format(new Date()));
            waterMarkTimeStamp = Math.max(waterMarkTimeStamp,previousElementTimestamp);
            return previousElementTimestamp;
        }
        return 0L;
    }
}
```

- ①.ConsumThreadFactory类以Runnable为接口，然后通过构造函数来获取相关参数，重写run()方法；
  ②.定义一个FlinkKafkaConsumer010对象的消费者，通过KafkaConsumerFactory类的getKafkaConsumer(bootstrapServer, topic, groupId)方法来获取一个初始化的消费者；
  ③.调用assignTimestampsAndWatermarks(new MessageWaterEmitterUtils())方法，以新建的MessageWaterEmitterUtils()为参数，来配置该消费者的水位线；
  ④.定义一个DataStream数据流对象dataStream，调用StreamEnvironment类的getEnvironment()方法来获取全局的环境对象，通过对其定义数据源、数据切分方法、逻辑操作等来将数据保存到dataStream中，MessageSplitterUtils()方法为自定义的数据行切分方法类；
  ⑤.定义一个Sink对象，此处为StreamingFileSink对象，也可以选择BucketingSink对象，不过在往后的版本会删除，推荐使用此处为StreamingFileSink对象来定义，具体的初始化配置请看脚本备注，通过根据不同的输出端，应选择不同的实体来定义，具体可查看Flink官网连接案例；
  ⑥.通过dataStream.addSink(sink)来指定sink，然后调用StreamEnvironmentgetEnvironment().execute("job名称")来执行程序；

[Flink官网连接案例]: https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/connectors/filesystem_sink.html

```java
package com.xmaviation.factory;

import com.xmaviation.environment.StreamEnvironment;
import com.xmaviation.utils.MessageSplitterUtils;
import com.xmaviation.utils.MessageWaterEmitterUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import java.util.concurrent.TimeUnit;


public class ConsumThreadFactory implements Runnable {
    private String bootstrapServer, topic, sinkPath;
    private int groupId;

    public ConsumThreadFactory(String bootstrapServer, String topic, int groupId, String sinkPath) {
        this.bootstrapServer = bootstrapServer;
        this.topic = topic;
        this.groupId = groupId;
        this.sinkPath = sinkPath;
    }

    @SneakyThrows
    @Override
    public void run() {
        FlinkKafkaConsumer010 consumer = KafkaConsumerFactory.getKafkaConsumer(bootstrapServer, topic, groupId);
        consumer.assignTimestampsAndWatermarks(new MessageWaterEmitterUtils());
        DataStream<Tuple2<String,Long>> dataStream =
                StreamEnvironment.getEnvironment().addSource(consumer)//添加数据源
                        .flatMap(new MessageSplitterUtils())//指定数据行切分方法
                        .keyBy(0)//字段下标以0开始，0表示指定第一个字段为聚合字段
//                        .countWindow(30000)//30000条数据为一个统计窗口
                        .timeWindow(Time.seconds(3))//3s为一个统计窗口
                .apply(new WindowFunction<Tuple2<String, Long>, Tuple2<String, Long>, Tuple, TimeWindow>() {
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<Tuple2<String, Long>> out) throws Exception {
                        long sum = 0L;
                        for (Tuple2<String, Long> record: input) {
                            sum += record.f1;
                        }
                        Tuple2<String, Long> result = input.iterator().next();
                        result.f1 = sum;
                        out.collect(result);
                    }
                });
                dataStream.print();
        StreamingFileSink<Tuple2<String,Long>> sink = StreamingFileSink
                .forRowFormat(new Path(sinkPath),
                        new SimpleStringEncoder<Tuple2<String,Long>>("UTF-8"))
/*

设置桶分配政策
ketAssigner：默认的桶分配政策，默认基于时间的分配器，每小时产生一个桶，格式如下yyyy-MM-dd--HH
BasePathBucketAssigner：将所有部分文件（part file）存储在基本路径中的分配器（单个全局桶）*/
                        .withBucketAssigner(new DateTimeBucketAssigner<>())
        //              .withBucketAssigner(new BasePathBucketAssigner<>())
                        

/* 有三种滚动政策
RollingPolicy
DefaultRollingPolicy
ntRollingPolicy
*/
                          .withRollingPolicy(
/**
滚动策略决定了写出文件的状态变化过程：

1. In-progress ：当前文件正在写入中
2. Pending ：当处于 In-progress 状态的文件关闭（closed）了，就变为 Pending 状态
3. Finished ：在成功的 Checkpoint 后，Pending 状态将变为 Finished 状态

观察到的现象：
1.会根据本地时间和时区，先创建桶目录
2.文件名称规则：part-<subtaskIndex>-<partFileIndex>
3.在macos中默认不显示隐藏文件，需要显示隐藏文件才能看到处于In-progress和Pending状态的文件，因为文件是按照.开头命名的*/
            DefaultRollingPolicy.create()
            .withRolloverInterval(TimeUnit.SECONDS.toMillis(60)) //设置滚动间隔,至少包含多久的数据才滚动处于In-progress 状态的部分文件
            .withInactivityInterval(TimeUnit.SECONDS.toMillis(60)) //设置不活动时间间隔，到时滚动处于In-progress 状态的部分文件
            .withMaxPartSize(1024 * 1024 * 1024) // part文件最大大小
            .build())
                    .withBucketCheckInterval(TimeUnit.SECONDS.toMillis(60)) // 桶检查间隔，这里设置为1s
                    .build();
        //        BucketingSink<Tuple2<String,Long>> sink = new BucketingSink<Tuple2<String,Long>>(sinkPath);
        //        sink.setBucketer(new DateTimeBucketer<Tuple2<String,Long>>("yyyy-MM-dd", ZoneId.of("Asia/Shanghai")));
        //        sink.setWriter(new StringWriter());
        //        sink.setBatchSize(1024 * 1024 * 400L); // this is 400 MB,
        //        sink.setBatchRolloverInterval(TimeUnit.SECONDS.toMillis(60)); // this is 60 mins
        //        sink.setPendingPrefix("");
        //        sink.setPendingSuffix("");
        //        sink.setInProgressPrefix(".");
        ////        StreamingFileSink<AirportRiskEntity> sink1 = StreamingFileSink
        ////                .forBulkFormat(new Path(sinkPath), ParquetAvroWriters.forReflectRecord(AirportRiskEntity.class))
        ////                .withBucketAssigner(bucketAssigner).build();
            dataStream.addSink(sink);
            StreamEnvironment.getEnvironment().execute("SaveToHdfs_"+groupId);
        }
        }
```

- ProduceThreadGroup类，赋值定义，循环调度

```java
package com.xmaviation.group;

import com.xmaviation.factory.ProduceThreadFactory;

import java.util.ArrayList;

public class ProduceThreadGroup {
    private ArrayList<Thread> produceThreadGroup = new ArrayList<>();
    public void produceThreadStart(){
        ProduceThreadFactory produce1 = new ProduceThreadFactory("D:\\ProductData\\AirportRiskServer.txt","AirportRiskServer1",1);
        ProduceThreadFactory produce2 = new ProduceThreadFactory("D:\\ProductData\\AirportRiskServer.txt","AirportRiskServer2",2);
        ProduceThreadFactory produce3 = new ProduceThreadFactory("D:\\ProductData\\AirportRiskServer.txt","AirportRiskServer3",3);

        produceThreadGroup.add(new Thread(produce1));
        produceThreadGroup.add(new Thread(produce2));
        produceThreadGroup.add(new Thread(produce3));

        for (Thread a : produceThreadGroup){
            System.out.println("Produce-Thread-" + a + "ready start");
            a.start();
            System.out.println("Produce-Thread-" + a + "already start");
        }
    }
}
```

- ConsumThreadGroup类，赋值定义，循环调度

```java
package com.xmaviation.group;

import com.xmaviation.factory.ConsumThreadFactory;

import java.util.ArrayList;


public class ConsumThreadGroup {
    private ArrayList<ConsumThreadFactory> consumThreadgroup = new ArrayList<ConsumThreadFactory>();
    public void consumThreadStart(){
        ConsumThreadFactory consum1 = new ConsumThreadFactory("***.***.**.**:9092","AirportRiskServer1",1,"/user/kafka/log/consum_log_1");
        ConsumThreadFactory consum2 = new ConsumThreadFactory("***.***.**.**:9092","AirportRiskServer2",2,"/user/kafka/log/consum_log_2");
        ConsumThreadFactory consum3 = new ConsumThreadFactory("***.***.**.**:9092","AirportRiskServer3",3,"/user/kafka/log/consum_log_3");
        consumThreadgroup.add(consum1);
        consumThreadgroup.add(consum2);
        consumThreadgroup.add(consum3);
        for (ConsumThreadFactory a:consumThreadgroup) {
            System.out.println("Consum-Thread-" + a + "ready start");
            new Thread(a).start();
            System.out.println("Consum-Thread-" + a + "already start");
        }
    }
}
```

- KafkaControl主类，实体化线程组类，批量启动线程

```java
package com.xmaviation;
import com.xmaviation.group.ConsumThreadGroup;
import com.xmaviation.group.ProduceThreadGroup;

import org.apache.log4j.Logger;

public class KafkaControl {
    private static Logger logger = Logger.getLogger(KafkaControl.class);
    public static void main(String[] args) {
        ConsumThreadGroup consumThreadGroup = new ConsumThreadGroup();
        consumThreadGroup.consumThreadStart();
        ProduceThreadGroup produceThreadGroup = new ProduceThreadGroup();
        produceThreadGroup.produceThreadStart();
    }
}
```

- pom.xml文件

```java
<project
        xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/maven-v4_0_0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <groupId>com.xmaviation</groupId>
    <artifactId>kafka_flink</artifactId>
    <version>1.0-SNAPSHOT</version>
    <properties>
        <scala.version>2.11.8</scala.version>
        <flink.version>1.9.0</flink.version>
        <kafka.version>2.3.0</kafka.version>
        <hadoop.version>3.1.1</hadoop.version>
    </properties>
    <!--<repositories>-->
        <!--<repository>-->
            <!--<id>scala-tools.org</id>-->
            <!--<name>Scala-Tools Maven2 Repository</name>-->
            <!--<url>http://scala-tools.org/repo-releases</url>-->
        <!--</repository>-->
    <!--</repositories>-->
    <!--<pluginRepositories>-->
        <!--<pluginRepository>-->
            <!--<id>scala-tools.org</id>-->
            <!--<name>Scala-Tools Maven2 Repository</name>-->
            <!--<url>http://scala-tools.org/repo-releases</url>-->
        <!--</pluginRepository>-->
    <!--</pluginRepositories>-->
    <dependencies>
        <!-- https://mvnrepository.com/artifact/com.alibaba/fastjson -->
        <dependency>
            <groupId>com.alibaba</groupId>
            <artifactId>fastjson</artifactId>
            <version>1.2.68</version>
            <scope>compile</scope>
        </dependency>
        <!-- https://mvnrepository.com/artifact/log4j/log4j -->
        <dependency>
            <groupId>log4j</groupId>
            <artifactId>log4j</artifactId>
            <version>1.2.17</version>
        </dependency>

        <!--<dependency>-->
            <!--<groupId>org.apache.cassandra</groupId>-->
            <!--<artifactId>cassandra-all</artifactId>-->
            <!--<version>0.8.1</version>-->
            <!--<scope>compile</scope>-->
            <!--<exclusions>-->
                <!--<exclusion>-->
                    <!--<groupId>org.slf4j</groupId>-->
                    <!--<artifactId>slf4j-log4j12</artifactId>-->
                <!--</exclusion>-->
                <!--<exclusion>-->
                    <!--<groupId>log4j</groupId>-->
                    <!--<artifactId>log4j</artifactId>-->
                <!--</exclusion>-->
            <!--</exclusions>-->
        <!--</dependency>-->
        <dependency>
            <groupId>org.scala-lang</groupId>
            <artifactId>scala-library</artifactId>
            <version>${scala.version}</version>
        </dependency>
        <dependency>
            <groupId>junit</groupId>
            <artifactId>junit</artifactId>
            <version>4.4</version>
            <scope>test</scope>
        </dependency>
        <!--<dependency>-->
        <!--<groupId>org.specs</groupId>-->
        <!--<artifactId>specs</artifactId>-->
        <!--<version>1.2.5</version>-->
        <!--<scope>test</scope>-->
        <!--</dependency>-->
        <!-- https://mvnrepository.com/artifact/org.apache.kafka/kafka_2.11 -->
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka_2.12</artifactId>
            <version>${kafka.version}</version>
            <scope>compile</scope>
        </dependency>
        <!-- https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients -->
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-clients</artifactId>
            <version>${kafka.version}</version>
            <scope>compile</scope>
        </dependency>
        <!-- https://mvnrepository.com/artifact/org.apache.kafka/kafka-streams -->
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-streams</artifactId>
            <version>${kafka.version}</version>
            <scope>compile</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-core</artifactId>
            <version>${flink.version}</version>
            <scope>compile</scope>
        </dependency>
        <!-- https://mvnrepository.com/artifact/org.apache.flink/flink-connector-kafka-0.10_2.11 -->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-connector-kafka-0.10_2.11</artifactId>
            <version>${flink.version}</version>
            <scope>compile</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-java</artifactId>
            <version>${flink.version}</version>
            <scope>compile</scope>
        </dependency>
        <!-- flink-streaming的jar包，2.11为scala版本号 -->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-streaming-java_2.11</artifactId>
            <version>${flink.version}</version>
            <scope>compile</scope>
        </dependency>
        <!-- https://mvnrepository.com/artifact/org.apache.flink/flink-clients -->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-clients_2.11</artifactId>
            <version>${flink.version}</version>
            <scope>compile</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-connector-filesystem_2.11</artifactId>
            <version>${flink.version}</version>
            <scope>compile</scope>
        </dependency>
        <!-- https://mvnrepository.com/artifact/org.apache.flink/flink-runtime -->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-runtime_2.12</artifactId>
            <version>${flink.version}</version>
            <scope>compile</scope>
        </dependency>

        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-parquet</artifactId>
            <version>1.7.0</version>
            <scope>compile</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-hadoop-compatibility_2.11</artifactId>
            <version>${flink.version}</version>
            <scope>compile</scope>
        </dependency>
        <!-- https://mvnrepository.com/artifact/org.apache.flink/flink-runtime-web，启用基于网络的作业提交 -->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-runtime-web_2.12</artifactId>
            <version>${flink.version}</version>
            <scope>compile</scope>
        </dependency>
        <dependency>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
            <version>1.18.10</version>
            <scope>compile</scope>
        </dependency>
        <dependency>
            <groupId>org.mortbay.jetty</groupId>
            <artifactId>jetty-util</artifactId>
            <version>6.1.26.cloudera.4</version>
            <scope>compile</scope>
        </dependency>
         <!--https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-common-->
        <!--<dependency>-->
            <!--<groupId>org.apache.hadoop</groupId>-->
            <!--<artifactId>hadoop-common</artifactId>-->
            <!--<version>${hadoop.version}</version>-->
            <!--<scope>compile</scope>-->
        <!--</dependency>-->
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-hdfs</artifactId>
            <version>${hadoop.version}</version>
            <scope>compile</scope>
        </dependency>
        <!-- https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-client -->
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-client</artifactId>
            <version>${hadoop.version}</version>
            <scope>compile</scope>
        </dependency>
    </dependencies>
    <build>
        <resources>
            <resource>
                <directory>${project.basedir}/src/main/resources</directory>
            </resource>
        </resources>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-shade-plugin</artifactId>
                <executions>
                    <execution>
                        <phase>package</phase>
                        <goals>
                            <goal>shade</goal>
                        </goals>
                        <configuration>
                            <transformers>
                                <transformer
                                        implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                                    <mainClass>com.xmaviation.KafkaControl</mainClass>
                                </transformer>
                                <!--若使用spring framework导致执行jar包报读取XML schema文件出错则添加以下内容
                                <transformer implementation="org.apache.maven.plugins.shade.resource.AppendingTransformer">
                                    <resource>META-INF/spring.handlers</resource>
                                </transformer>
                                <transformer implementation="org.apache.maven.plugins.shade.resource.AppendingTransformer">
                                    <resource>META-INF/spring.schemas</resource>
                                </transformer>-->
                            </transformers>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
```