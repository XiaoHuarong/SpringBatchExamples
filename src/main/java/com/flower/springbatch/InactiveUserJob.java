package com.flower.springbatch;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import javax.annotation.Resource;

/**
 * Job: Check and make user inactive end of every month if the user's inactive duration exceed the Expired-Time (24<=t<36)(month).
 */
public class InactiveUserJob implements Job {


    private Logger log = LoggerFactory.getLogger(this.getClass());


    @Resource(name = "databaseToFileJob")
    private org.springframework.batch.core.Job inactiveDataToFileJob;

    /*@Qualifier("updateConsumerLoyalPointsToDBJob")
    private org.springframework.batch.core.Job updatePointsJob;*/

    @Autowired
    private JobLauncher jobLauncher;


    @Override
    public void execute(JobExecutionContext jobExecutionContext) {
        log.info("=========================== Job: Make user inactive if the user is inactive from expired time {}: =====================", System.currentTimeMillis());


        try {


            makeAMUsersInactiveAndDeletePIIByLastActivityDate();


        } catch (JobParametersInvalidException | JobExecutionAlreadyRunningException | JobRestartException | JobInstanceAlreadyCompleteException e) {
            e.printStackTrace();
        }
    }

    /**
     * Make user inactive
     * For Non-Japan,Non-Korea market, last activity date exceed 24 months.
     * For Japan market, last activity date exceed 13 months.
     */
    public void makeAMUsersInactiveAndDeletePIIByLastActivityDate() throws JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {

//        jobLauncher.run(updatePointsJob, updatePointsJobParameters());

        jobLauncher.run(inactiveDataToFileJob, inactiveDataToFileJobParameters());

    }

    private JobParameters inactiveDataToFileJobParameters() {

        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();

        jobParametersBuilder.addLong("time", System.currentTimeMillis());
        return jobParametersBuilder.toJobParameters();
    }

    private JobParameters updatePointsJobParameters() {
        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addLong("time", System.currentTimeMillis());
        return jobParametersBuilder.toJobParameters();
    }
}
