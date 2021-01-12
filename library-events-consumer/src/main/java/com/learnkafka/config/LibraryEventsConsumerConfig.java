package com.learnkafka.config;

import com.learnkafka.service.LibraryEventsService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

@Configuration
@EnableKafka
@Slf4j
public class LibraryEventsConsumerConfig {
    @Autowired
    LibraryEventsService libraryEventsService;

    @Bean
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(ConcurrentKafkaListenerContainerFactoryConfigurer configurer, ConsumerFactory<Object, Object> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, consumerFactory);
        factory.setConcurrency(3);

        factory.setErrorHandler((ex, data) -> {
            log.info("Exception in consumerConfig is {} and the record is {}", ex.getMessage(), data);
        });

        factory.setRetryTemplate(retryTemplate());

        factory.setRecoveryCallback(
                context -> {
                    if (!(context.getLastThrowable().getCause() instanceof RecoverableDataAccessException)) {
                        log.info("Inside the non recoverable logic");

                        throw new RuntimeException(context.getLastThrowable().getMessage());
                    }

                    log.info("Inside the recoverable logic");

                    ConsumerRecord<Integer, String> consumerRecord = (ConsumerRecord<Integer, String>) context.getAttribute("record");
                    libraryEventsService.handleRecovery(consumerRecord);

                    return null;
                }
        );

        return factory;
    }

    private RetryTemplate retryTemplate() {
        RetryTemplate retryTemplate = new RetryTemplate();

        retryTemplate.setRetryPolicy(retryPolicy());

        retryTemplate.setBackOffPolicy(backoffPolicy());

        return retryTemplate;
    }

    private BackOffPolicy backoffPolicy() {
        FixedBackOffPolicy fixedBackOffPolicy = new FixedBackOffPolicy();

        fixedBackOffPolicy.setBackOffPeriod(1000);

        return fixedBackOffPolicy;
    }

    private RetryPolicy retryPolicy() {
        SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy(3, retryableExceptions(), true);

        simpleRetryPolicy.setMaxAttempts(3);

        return simpleRetryPolicy;
    }

    private Map<Class<? extends Throwable>, Boolean> retryableExceptions() {
        HashMap<Class<? extends Throwable>, Boolean> retryableExceptions = new HashMap<>();

        retryableExceptions.put(IllegalArgumentException.class, false);
        retryableExceptions.put(RecoverableDataAccessException.class, true);

        return retryableExceptions;
    }

}
