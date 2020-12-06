package com.raj.nola.config;

public class SystemConfig {

    public final static String consumerApplicationID = "CustomerConsumerApp";
    public final static String bootstrapServers = "localhost:9092,localhost:9093";
    public final static String topicName = "customer-demo-topic";
    public final static String groupID = "JsonCustomerReaderGroup";
}
