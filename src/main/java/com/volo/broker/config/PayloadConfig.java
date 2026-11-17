package com.volo.broker.config;

import com.volo.broker.buffer.HeapPayloadAllocator;
import com.volo.broker.buffer.NettyPayloadAllocator;
import com.volo.broker.buffer.PayloadAllocator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(PayloadMemoryProperties.class)
public class PayloadConfig {

    @Bean
    @ConditionalOnMissingBean
    public PayloadAllocator payloadAllocator(PayloadMemoryProperties properties) {
        if (properties.mode() == PayloadMemoryProperties.PayloadMode.POOLED_DIRECT) {
            return new NettyPayloadAllocator(properties.inlineThresholdBytes());
        }
        return new HeapPayloadAllocator();
    }
}
