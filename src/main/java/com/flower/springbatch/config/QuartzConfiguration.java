package com.flower.springbatch.config;


import com.flower.springbatch.InactiveUserJob;
import org.quartz.JobDetail;
import org.quartz.SimpleTrigger;
import org.quartz.spi.JobFactory;
import org.quartz.spi.TriggerFiredBundle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.quartz.CronTriggerFactoryBean;
import org.springframework.scheduling.quartz.JobDetailFactoryBean;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;
import org.springframework.scheduling.quartz.SpringBeanJobFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.Arrays;

@Configuration
public class QuartzConfiguration {

    private final Logger log = LoggerFactory.getLogger(QuartzConfiguration.class);

    @Bean
    public JobFactory jobFactory(ApplicationContext applicationContext) {

        QuartzJobFactory quartzJobFactory = new QuartzJobFactory();
        quartzJobFactory.setApplicationContext(applicationContext);
        return quartzJobFactory;
    }

    @Bean
    public SchedulerFactoryBean schedulerFactoryBean(ApplicationContext applicationContext) {


        try {
            Thread.sleep(10 * 1000L);
        } catch (Exception ex) {
            log.error(Arrays.toString(ex.getStackTrace()));
        }

        SchedulerFactoryBean factory = new SchedulerFactoryBean();

        factory.setOverwriteExistingJobs(true);
        factory.setJobFactory(jobFactory(applicationContext));

//        factory.setDataSource(dataSource);

//        factory.setQuartzProperties(quartzProperties);
        factory.setTriggers(inactiveUserTrigger().getObject());

        return factory;
    }





    /*===================== JobDetail and Trigger configuration =====================*/


    @Bean(name = "inactiveUserJob")
    public JobDetailFactoryBean inactiveUserJob() {

        JobDetailFactoryBean jobDetailFactoryBean = new JobDetailFactoryBean();
        jobDetailFactoryBean.setJobClass(InactiveUserJob.class);
        jobDetailFactoryBean.setDescription("----------------");
        jobDetailFactoryBean.setDurability(true);
        jobDetailFactoryBean.setName("inactiveUserKeyName");
        return jobDetailFactoryBean;
    }

    /////////








    /////////////

    @Bean(name = "inactiveUserTrigger")
    public CronTriggerFactoryBean inactiveUserTrigger() {
        return createCronTrigger(inactiveUserJob().getObject(), "1/10 * * * * ?");
    }
    private static CronTriggerFactoryBean createCronTrigger(JobDetail jobDetail, String cronExpression) {
        CronTriggerFactoryBean factoryBean = new CronTriggerFactoryBean();
        factoryBean.setJobDetail(jobDetail);
        factoryBean.setCronExpression(cronExpression);
        factoryBean.setMisfireInstruction(SimpleTrigger.MISFIRE_INSTRUCTION_FIRE_NOW);
        return factoryBean;
    }

}


