package org.apache.doris.avro;

import com.google.common.base.Optional;
import com.google.common.collect.Queues;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.io.NullWritable;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;

public class KafkaReader extends AvroReader {
    private static final Logger LOG = LogManager.getLogger(KafkaReader.class);
    private final Properties props = new Properties();
    private final Map<String, String> requiredParams;
    private final String kafkaSchemaPath;
    private Schema schema;
    private final String topic;
    private long startOffset;
    private long maxRows;
    private long readRows;
    private int partition;
    private final BlockingQueue<Optional<ConsumerRecord<String, GenericRecord>>> recordsQueue;
    private final int maxRunTimeSec = 15;
    private Thread consumerThread;
    private boolean eos = false;

    public KafkaReader(Map<String, String> requiredParams) {
        this.requiredParams = requiredParams;
        this.kafkaSchemaPath = requiredParams.get(AvroProperties.KAFKA_SCHEMA_PATH);
        this.topic = requiredParams.get(AvroProperties.KAFKA_TOPIC);
        this.recordsQueue = Queues.newLinkedBlockingQueue(1000);
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
        consumerThread = new Thread(new ConsumerTask(topic, partition, startOffset, maxRows, props, maxRunTimeSec, recordsQueue));
        consumerThread.start();
    }

    public static class ConsumerTask implements Runnable {
        private final String topic;
        private final int partition;
        private final long startOffset;
        private final long maxRows;
        private final Properties props;
        private final BlockingQueue<Optional<ConsumerRecord<String, GenericRecord>>> recordsQueue;
        private final long maxRunTimeSec;
        private long readRows = 0;

        public ConsumerTask(String topic, int partition, long startOffset,
                long maxRows, Properties props, long maxRunTimeSec,
                BlockingQueue<Optional<ConsumerRecord<String, GenericRecord>>> recordsQueue) {
            this.topic = topic;
            this.partition = partition;
            this.startOffset = startOffset;
            this.maxRows = maxRows;
            this.props = props;
            this.maxRunTimeSec = maxRunTimeSec;
            this.recordsQueue = recordsQueue;
        }

        public void run() {
            KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props);
            int emptyCounter = 0;
            try {
                TopicPartition topicPartition = new TopicPartition(topic, partition);
                consumer.assign(Collections.singletonList(topicPartition));
                // long position = consumer.position(topicPartition);
                // if (position < startOffset) {
                //     LOG.warn("The position of Kafka's topic=" + topic + " and partition=" + partition
                //             + " is less than the starting offset. Partition position=" + position + "");
                //     // startOffset = position + 1;
                // }
                consumer.seek(topicPartition, startOffset);
                long curTime = System.currentTimeMillis();
                while (readRows < maxRows) {
                    // When the data set in the partition from startOffset to the latest offset does not reach the data volume of maxRows.
                    // After the reading timeout period is reached, the consumer program is actively released.
                    if (System.currentTimeMillis() - curTime > maxRunTimeSec * 1000) {
                        break;
                    }
                    ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(200));
                    if (records.isEmpty()) {
                        emptyCounter++;
                        if (emptyCounter > 5) {
                            break;
                        }
                        continue;
                    }
                    for (ConsumerRecord<String, GenericRecord> record : records) {
                        if (readRows < maxRows) {
                            recordsQueue.put(Optional.of(record));
                        }
                        readRows++;
                    }
                }
            } catch (Throwable t) {
                LOG.warn("failed to consumer kafka data, topic " + topic
                        + ", partition " + partition
                        + ", startOffset: " + startOffset, t);
            } finally {
                consumer.close();
                try {
                    recordsQueue.put(Optional.fromNullable(null));
                } catch (InterruptedException e) {
                    LOG.warn("failed to put null to queue", e);
                }
            }
        }
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    @Override
    public boolean hasNext(AvroWrapper<Pair<Integer, Long>> inputPair, NullWritable ignore) throws IOException {
        // return CollectionUtils.isNotEmpty(recordsQueue);
        return true;
    }

    @Override
    public Object getNext() {
        if (eos) {
            return null;
        }
        Optional<ConsumerRecord<String, GenericRecord>> record = null;
        try {
            record = recordsQueue.take();
        } catch (InterruptedException e) {
            LOG.warn("failed to take record from queue", e);
            eos = true;
            return null;
        }
        if (!record.isPresent()) {
            eos = true;
            return null;
        }
        ConsumerRecord<String, GenericRecord> consumerRecord = record.get();
        GenericRecord recordValue = consumerRecord.value();
        int partition = consumerRecord.partition();
        String key = consumerRecord.key();
        long offset = consumerRecord.offset();
        LOG.info("partition=" + partition + " offset =" + offset + " key=" + key + " value="
                + recordValue.toString());
        return recordValue;
    }

    @Override
    public void close() throws IOException {
        if (consumerThread != null) {
            try {
                consumerThread.join();
            } catch (InterruptedException e) {
                LOG.warn("interrupted exception when close kafka consumer thread", e);
            }
        }
    }
}

