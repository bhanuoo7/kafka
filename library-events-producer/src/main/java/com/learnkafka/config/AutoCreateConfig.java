package com.learnkafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.TopicBuilder;

//this type of code is not recommended in prod
@Configuration
@Profile("local") //this will be invoked in local only not prod
public class AutoCreateConfig {

    @Bean
    public NewTopic libraryEvents(){

        return TopicBuilder.name("library-events").partitions(3).replicas(1).build();
    }


}
