package com.raj.nola.consumer;

import com.raj.nola.config.SystemConfig;
import com.raj.nola.types.Customer;
import com.raj.nola.util.JsonDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class CustomerConsumer {

    public static void main(String[] args) {

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, SystemConfig.consumerApplicationID);
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SystemConfig.bootstrapServers);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerProps.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, Customer.class);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, SystemConfig.groupID);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<Integer, Customer> consumer = new KafkaConsumer<Integer, Customer>(consumerProps);

        consumer.subscribe(Arrays.asList(SystemConfig.topicName));


        while (true) {
            ConsumerRecords<Integer, Customer>  records = consumer.poll(Duration.ofMillis(200));
            records.forEach(record -> System.out.println(record.value()));
        }

    }
}
