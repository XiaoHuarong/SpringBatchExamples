package com.flower.springbatch.batchjob;

import com.flower.springbatch.common.TestUserDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

/**
 */
public class FileToDBProcessor implements ItemProcessor <TestUserDTO,TestUserDTO>{

    private static final Logger LOGGER = LoggerFactory.getLogger(FileToDBProcessor.class);

    @Override
    public TestUserDTO process(TestUserDTO testUserDTO) throws Exception {
        LOGGER.info("Async User: {}", testUserDTO.getUserId());
        //Process
        testUserDTO.setPassword(testUserDTO.getPassword() + "99999");

        //Data filter, not write to DB.
        if (testUserDTO.getUserId().equals("5")){
            return null;
        } else {
            return testUserDTO;
        }

    }
}
