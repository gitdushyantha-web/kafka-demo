package iot;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ApplicationRecoverableException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.TransactionAbortableException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.time.Duration.ofSeconds;
import static java.util.Collections.singleton;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.TRANSACTIONAL_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class TransactionalClient {

    private static final String CONSUMER_GROUP_ID = "my-group-id";
    private static final String OUTPUT_TOPIC = "iotoutputtopic";
    private static final String INPUT_TOPIC = "iotinputtopic";

    private static KafkaConsumer<String, String> consumer;
    private static KafkaProducer<String, String> producer;
    private static volatile boolean isRunning = true;

    public static void main(String[] args) {
        Utils.printOut("Starting TransactionalClient");

        registerShutdownHook();

        initializeApplication();

        while (isRunning) {
            try {
                try {
                    Utils.printOut("Polling records from Kafka");
                    ConsumerRecords<String, String> records = consumer.poll(ofSeconds(5));

                    Utils.printOut("Polled %d records from input topic '%s'", records.count(), INPUT_TOPIC);
                    for (ConsumerRecord<String, String> record : records) {
                        Utils.printOut("Record: key='%s', value='%s', partition=%d, offset=%d", record.key(), record.value(), record.partition(), record.offset());
                    }

                    Map<String, Integer> messageMap = new HashMap<>();
                    for (ConsumerRecord<String, String> record : records) {
                        System.out.print(record.value());
                        String[] messages = record.value().split(" ");
                        for (String msg : messages) {
                            messageMap.merge(msg, 1, Integer::sum);
                        }
                    }

                    Utils.printOut("Message count to be produced: %s", messageMap);

                    Utils.printOut("Beginning transaction");
                    producer.beginTransaction();

                    messageMap.forEach((key, value) ->
                        producer.send(new ProducerRecord<>(OUTPUT_TOPIC, key, value.toString())));
                    Utils.printOut("Produced %d message count records to output topic '%s'", messageMap.size(), OUTPUT_TOPIC);

                    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
                    for (TopicPartition partition : records.partitions()) {
                        List<ConsumerRecord<String, String>> partitionedRecords = records.records(partition);
                        long offset = partitionedRecords.get(partitionedRecords.size() - 1).offset();
                        offsetsToCommit.put(partition, new OffsetAndMetadata(offset + 1));
                    }

                    producer.sendOffsetsToTransaction(offsetsToCommit, consumer.groupMetadata());
                    Utils.printOut("Sent offsets to transaction for commit");

                    producer.commitTransaction();
                    Utils.printOut("Transaction committed successfully");
                } catch (TransactionAbortableException e) {
                    Utils.printErr("TransactionAbortableException: %s. Aborting transaction.", e.getMessage());
                    producer.abortTransaction();
                    Utils.printOut("Transaction aborted. Resetting consumer to last committed positions.");
                    resetToLastCommittedPositions(consumer);
                }
            } catch (InvalidConfigurationException e) {
                Utils.printErr("InvalidConfigurationException: %s. Shutting down.", e.getMessage());
                closeAll();
                throw e;
            } catch (ApplicationRecoverableException e) {
                Utils.printErr("ApplicationRecoverableException: %s. Restarting application.", e.getMessage());
                closeAll();
                initializeApplication();
            } catch (KafkaException e) {
                Utils.printErr("KafkaException: %s. Restarting application.", e.getMessage());
                closeAll();
                initializeApplication();
            } catch (Exception e) {
                Utils.printErr("Unhandled exception: %s", e.getMessage());
                closeAll();
                throw e;
            }
        }
    }

    public static void initializeApplication() {
        Utils.printOut("Initializing Kafka consumer and producer");
        consumer = createKafkaConsumer();
        producer = createKafkaProducer();
        
        producer.initTransactions();
        Utils.printOut("Producer initialized with transactions");
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
        props.put(ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(singleton(INPUT_TOPIC));
        return consumer;
    }

    private static KafkaProducer<String, String> createKafkaProducer() {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(TRANSACTIONAL_ID_CONFIG, "prod-1");
        props.put(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<>(props);
    }

    private static void resetToLastCommittedPositions(KafkaConsumer<String, String> consumer) {
        Utils.printOut("Resetting consumer to last committed positions");
        final Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(consumer.assignment());
        consumer.assignment().forEach(tp -> {
            OffsetAndMetadata offsetAndMetadata = committed.get(tp);
            if (offsetAndMetadata != null)
                consumer.seek(tp, offsetAndMetadata.offset());
            else
                consumer.seekToBeginning(singleton(tp));
        });
    }

    private static void closeAll() {
        if (consumer != null) {
            consumer.close();
        }
        if (producer != null) {
            producer.close();
        }
        Utils.printOut("All resources closed");
    }

    private static void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Utils.printOut("Shutdown signal received. Exiting...");
            isRunning = false;
            closeAll();
        }));
    }

}