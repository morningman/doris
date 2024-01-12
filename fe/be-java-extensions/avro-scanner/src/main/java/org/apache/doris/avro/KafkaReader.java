package org.apache.doris.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapred.Pair;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Queue;

public class KafkaReader extends AvroReader {
    private static final Logger LOG = LogManager.getLogger(KafkaReader.class);
    private final Properties props = new Properties();
    private final Map<String, String> requiredParams;
    private final String kafkaSchemaPath;
    private KafkaConsumer<String, GenericRecord> consumer;
    private Schema schema;
    private final String topic;
    private long startOffset;
    private long maxRows;
    private long readRows;
    private int partition;
    private final Queue<ConsumerRecord<String, GenericRecord>> recordsQueue;
    private final int pollTimeout = 3000;

    public KafkaReader(Map<String, String> requiredParams) {
        this.requiredParams = requiredParams;
        this.kafkaSchemaPath = requiredParams.get(AvroProperties.KAFKA_SCHEMA_PATH);
        this.topic = requiredParams.get(AvroProperties.KAFKA_TOPIC);
        this.recordsQueue = new LinkedList<>();
    }

    @Override
    public void open(AvroFileContext avroFileContext, boolean tableSchema) throws IOException {
        if (tableSchema) {
            this.schema = new Schema.Parser().parse(new File(kafkaSchemaPath));
        } else {
            initKafkaProps();
            pollRecords();
        }
    }

    private void initKafkaProps() {
        this.partition = Integer.parseInt(requiredParams.get(AvroProperties.SPLIT_SIZE));
        this.startOffset = Long.parseLong(requiredParams.get(AvroProperties.SPLIT_START_OFFSET));
        this.maxRows = Long.parseLong(requiredParams.get(AvroProperties.SPLIT_FILE_SIZE));
        props.put(AvroProperties.KAFKA_BOOTSTRAP_SERVERS,
                requiredParams.get(AvroProperties.KAFKA_BROKER_LIST));
        props.put(AvroProperties.KAFKA_GROUP_ID, requiredParams.get(AvroProperties.KAFKA_GROUP_ID));
        props.put(AvroProperties.KAFKA_SCHEMA_REGISTRY_URL,
                requiredParams.get(AvroProperties.KAFKA_SCHEMA_REGISTRY_URL));
        props.put(AvroProperties.KAFKA_KEY_DESERIALIZER, AvroProperties.KAFKA_STRING_DESERIALIZER);
        props.put(AvroProperties.KAFKA_VALUE_DESERIALIZER, AvroProperties.KAFKA_AVRO_DESERIALIZER);
        props.put(AvroProperties.KAFKA_AUTO_COMMIT_ENABLE, "true");
    }

    private void pollRecords() {
        this.consumer = new KafkaConsumer<>(props);
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        consumer.assign(Collections.singletonList(topicPartition));
        // long position = consumer.position(topicPartition);
        // if (position < startOffset) {
        //     LOG.warn("The position of Kafka's topic=" + topic + " and partition=" + partition
        //             + " is less than the starting offset. Partition position=" + position + "");
        //     // startOffset = position + 1;
        // }
        consumer.seek(topicPartition, startOffset);
        long tmpPollTime = System.currentTimeMillis();
        while (readRows < maxRows) {
            // When the data set in the partition from startOffset to the latest offset does not reach the data volume of maxRows.
            // After the reading timeout period is reached, the consumer program is actively released.
            if (System.currentTimeMillis() - tmpPollTime > pollTimeout) {
                break;
            }
            ConsumerRecords<String, GenericRecord> records = consumer.poll(200);
            for (ConsumerRecord<String, GenericRecord> record : records) {
                if (readRows < maxRows) {
                    recordsQueue.offer(record);
                }
                tmpPollTime = System.currentTimeMillis();
                readRows++;
            }
        }
        consumer.close();
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    @Override
    public boolean hasNext(AvroWrapper<Pair<Integer, Long>> inputPair, NullWritable ignore) throws IOException {
        return CollectionUtils.isNotEmpty(recordsQueue);
    }

    @Override
    public Object getNext() {
        ConsumerRecord<String, GenericRecord> record = recordsQueue.poll();
        GenericRecord recordValue = Objects.requireNonNull(record).value();
        int partition = record.partition();
        String key = record.key();
        long offset = record.offset();
        LOG.info("partition=" + partition + " offset =" + offset + " key=" + key + " value="
                + recordValue.toString());
        return recordValue;
    }

    @Override
    public void close() throws IOException {
        if (Objects.nonNull(consumer)) {
            consumer.close();
        }
    }
}
