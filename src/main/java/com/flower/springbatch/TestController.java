package com.flower.springbatch;


import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.Date;

@Controller
@RequestMapping("/test")
public class TestController {

    private final Job job;

    private final JobLauncher jobLauncher;

    TestController(@Qualifier("databaseToFileJob") Job job, JobLauncher jobLauncher) {
        this.job = job;
        this.jobLauncher = jobLauncher;
    }


    @PostMapping("/sftp")
    @ResponseBody
    public void testSftpSpringBatch() throws JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {



        jobLauncher.run(job, newExecution());

    }

    private JobParameters newExecution() {
        /*Map<String, JobParameter> parameters = new HashMap<>();

        JobParameter parameter = new JobParameter(new Date());
        parameters.put("currentTime", parameter);*/

        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addDate("currentTime", new Date());
        jobParametersBuilder.addLong("memberId",2L);
        jobParametersBuilder.addString("demo","AAAAAAAAA");
//        jobParametersBuilder.toJobParameters();


        return jobParametersBuilder.toJobParameters();//new JobParameters(parameters);
    }
}
