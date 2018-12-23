package com.flower.springbatch.batchjob;

import com.flower.springbatch.common.TestUserDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

/**
 */
public class DBToFileProcessor implements ItemProcessor<TestUserDTO, TestUserDTO> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DBToFileProcessor.class);

    @Override
    public TestUserDTO process(TestUserDTO amUserDTO) throws Exception {
         LOGGER.info("Processing AMUser information: {}", amUserDTO.getUserId());
        return amUserDTO;
    }
}
