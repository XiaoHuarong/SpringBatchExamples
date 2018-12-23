package com.flower.springbatch.batchjob;

import com.flower.springbatch.batchjob.springintegration.sftp.SftpGetFileTasklet;
import com.flower.springbatch.batchjob.springintegration.sftp.SftpSendFileTasklet;
import com.flower.springbatch.batchjob.util.FileUtil;
import com.flower.springbatch.common.StringHeaderWriter;
import com.flower.springbatch.common.TestUserDTO;
import com.flower.springbatch.config.SftpConfiguration;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.*;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.*;
import org.springframework.batch.item.support.ClassifierCompositeItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.classify.BackToBackPatternClassifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.io.FileSystemResource;
import org.springframework.integration.file.filters.SimplePatternFileListFilter;
import org.springframework.integration.sftp.inbound.SftpInboundFileSynchronizer;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.messaging.MessageChannel;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.io.File;
import java.net.MalformedURLException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;

/**
 * Function：从SFTP服务器读取文件，并更新到数据库；从数据库根据数据内容的不同，导入到不同的文件之中。
 * 相关技术：Spring-integration-sftp
 * classifier composite writer
 * <p>
 * TODO:
 * <p>
 * 1. 文件能不能进行内容追加？, 还是批处理只能够导出到单独的新的文件。
 */
@Configuration
@EnableBatchProcessing
public class UserDataToMulFileJob {

    private Logger logger = LoggerFactory.getLogger(this.getClass());


    //Spring batch Job，@Bean没有name属性，bean以方法名区别。
    @Bean
    Job databaseToFileJob(JobBuilderFactory jobBuilderFactory,
                          @Qualifier("sftpFileGetStep") Step sftpFileGetStep,
                          @Qualifier("fileToDatabaseStep") Step fileToDatabaseStep,
                          @Qualifier("dbToMulFileStep") Step dbToMulFileStep,
                          @Qualifier("sftpFileSendStep") Step sftpFileSendStep) {
        JobBuilder jobBuilder = jobBuilderFactory.get("databaseToFileJob");
//        Job executorJob = jobBuilder.incrementer(new RunIdIncrementer()).flow(csvStudentStep).end().build();
        Job executorJob = jobBuilder.incrementer(new RunIdIncrementer())
                .start(sftpFileGetStep)
                .next(fileToDatabaseStep)
                .next(dbToMulFileStep)
                .next(sftpFileSendStep)
                .build();
        return executorJob;
    }


    /*---------------------  Step-1: Synchronize file from Sftp to local ---------------------*/

    @Resource(name = "sftpInbound")
    SftpInboundFileSynchronizer sftpFileSynchronize;

    /**
     * Step-1: get file from SFTP server.
     * <p>
     *
     * @return Step-1: Synchronize file from SFTP server into local.
     */
    @Bean
    Step sftpFileGetStep(StepBuilderFactory stepBuilderFactory, SftpGetFileTasklet sftpGetRemoteFilesTasklet) {

        SftpGetFileTasklet sftpGetRemoteFileTasklet = new SftpGetFileTasklet();
        return stepBuilderFactory.get("sftpFileGetStep").tasklet(sftpGetRemoteFilesTasklet).build();
    }

    /**
     * Define Tasklet: Synchronize file to local from SFTP server.
     */
    @Bean
    @StepScope
    SftpGetFileTasklet sftpGetRemoteFileTasklet(Environment environment) {
        SftpGetFileTasklet sftpTasklet = new SftpGetFileTasklet();
        //set retry to done.
        sftpTasklet.setRetryIfNotFound(true);
        sftpTasklet.setDownloadFileAttempts(3);
        sftpTasklet.setRetryIntervalMilliseconds(10000);

        //Set file need to synchronize
        sftpTasklet.setFileNamePattern(SftpConfiguration.getSftpRemoteFileFilter());

        //set SFTP remote and local directory
        sftpTasklet.setRemoteDirectory(SftpConfiguration.getSftpRemoteFileSendPath());
        FileUtil.checkOrCreateFile(SftpConfiguration.getSftpLocalFileGetPath(),true);

        sftpTasklet.setLocalDirectory(new File(SftpConfiguration.getSftpLocalFileGetPath()));

        sftpTasklet.setFtpInboundFileSynchronizer(sftpFileSynchronize);
        return sftpTasklet;
    }


    /*---------------------  Step-2: Read file into DB ---------------------*/

    /**
     * Step-2: Read file into DB, operate sql.
     * <p>
     *
     * @return Step-2: Update DB by reading file contents.
     */
    @Bean
    Step fileToDatabaseStep(ItemStreamReader<TestUserDTO> fileToDBItemReader,
                            ItemProcessor<TestUserDTO, TestUserDTO> fileToDBItemProcessor,
                            ItemWriter<TestUserDTO> fileToDBItemWriter,
                            StepBuilderFactory stepBuilderFactory) {
        return stepBuilderFactory.get("fileToDatabaseStep")
                .<TestUserDTO, TestUserDTO>chunk(1)
                .reader(fileToDBItemReader)
                .processor(fileToDBItemProcessor)
                .writer(fileToDBItemWriter)
                .build();
    }

    /**
     * Step-2: ItemReader
     * <p>
     * read single local file.
     * ItemStreamReader & ItemReader 实现的接口不同
     */
    @Bean
    @StepScope
    ItemStreamReader<TestUserDTO> fileToDBItemReader(Environment environment) throws MalformedURLException {
        FlatFileItemReader<TestUserDTO> csvFileReader = new FlatFileItemReader<>();

        //Todo read single file, such as "user.csv"
        File file = new File(SftpConfiguration.getSftpLocalFileGetPath() + "user.csv");
        //csvFileReader.setResource(new ClassPathResource(environment.getRequiredProperty(PROPERTY_CSV_SOURCE_FILE_PATH)));

        csvFileReader.setResource(new FileSystemResource(file));
        //Skip lines of file contents, no read
        csvFileReader.setLinesToSkip(1);

        //Set file content and <TestUserDTO> mapper
        LineMapper<TestUserDTO> userFileLineMapper = createUserLineMapper();
        csvFileReader.setLineMapper(userFileLineMapper);

        return csvFileReader;
    }

    private LineMapper<TestUserDTO> createUserLineMapper() {
        DefaultLineMapper<TestUserDTO> userFileLineMapper = new DefaultLineMapper<>();

        //Read file line content to field properties 设置每行内容读取成的字段
        LineTokenizer userFileLineTokenizer = createUserFileLineTokenizer();
        userFileLineMapper.setLineTokenizer(userFileLineTokenizer);

        //Config file content field mapper with bean <TestUserDTO> field 文件内容的字段与实体类的字段对应
        FieldSetMapper<TestUserDTO> userInformationMapper = createUserInformationMapper();
        userFileLineMapper.setFieldSetMapper(userInformationMapper);

        return userFileLineMapper;
    }

    private LineTokenizer createUserFileLineTokenizer() {
        DelimitedLineTokenizer userFileLineTokenizer = new DelimitedLineTokenizer();
        userFileLineTokenizer.setDelimiter(";");
        userFileLineTokenizer.setNames(new String[]{"userId", "userName", "password"});
        return userFileLineTokenizer;
    }

    private FieldSetMapper<TestUserDTO> createUserInformationMapper() {
        BeanWrapperFieldSetMapper<TestUserDTO> userInformationMapper = new BeanWrapperFieldSetMapper<>();
        userInformationMapper.setTargetType(TestUserDTO.class);
        return userInformationMapper;
    }

    /**
     * Step-2: ItemProcessor
     * <p>
     * After reading file, can process item data if necessary. Then write it.
     */
    @Bean
    ItemProcessor<TestUserDTO, TestUserDTO> fileToDBItemProcessor() {
        return new FileToDBProcessor();
    }

    //参数占位符，使用实体属性名称
    private String SQL_UPDATE_POINTS_BY_MEMBER_ID = "UPDATE sysuser SET password = :password WHERE userid = :userId";

    /**
     * Step-2: ItemWriter
     * <p>
     * write file content into DB.
     */
    @Bean
    @StepScope
    ItemWriter<TestUserDTO> fileToDBItemWriter(DataSource dataSource, NamedParameterJdbcTemplate jdbcTemplate) {
        JdbcBatchItemWriter<TestUserDTO> databaseItemWriter = new JdbcBatchItemWriter<>();
        databaseItemWriter.setDataSource(dataSource);
        databaseItemWriter.setJdbcTemplate(jdbcTemplate);

        databaseItemWriter.setSql(SQL_UPDATE_POINTS_BY_MEMBER_ID);

        ItemSqlParameterSourceProvider<TestUserDTO> sqlParameterSourceProvider = userUpdateSqlParameterSourceProvider();

        databaseItemWriter.setItemSqlParameterSourceProvider(sqlParameterSourceProvider);

        return databaseItemWriter;
    }

    private ItemSqlParameterSourceProvider<TestUserDTO> userUpdateSqlParameterSourceProvider() {

        return new BeanPropertyItemSqlParameterSourceProvider<>();
    }

    /*---------------------  Step-3: Export data to file from DB ---------------------*/


    /**
     * Step-3: Export data to file from DB
     * <p>
     * Export different data to different files.根据数据库中不同的数据类型，到导出到不同的文件中
     * 使用 ClassifierCompositeItemWriter 分类复合条目写
     */
    @Bean
    Step dbToMulFileStep(ItemReader<TestUserDTO> dbToMulFileItemReader,
                         ItemProcessor<TestUserDTO, TestUserDTO> dbToMulFileItemProcessor,
                         ClassifierCompositeItemWriter<TestUserDTO> classifierCompositeWriter,
                         StepBuilderFactory stepBuilderFactory,
                         FlatFileItemWriter<TestUserDTO> flatFileItemWriterLarge,
                         FlatFileItemWriter<TestUserDTO> flatFileItemWriterMiddle,
                         FlatFileItemWriter<TestUserDTO> flatFileItemWriterSmall) {
        return stepBuilderFactory.get("dbToMulFileStep")
                .<TestUserDTO, TestUserDTO>chunk(1)
                .reader(dbToMulFileItemReader)
                .processor(dbToMulFileItemProcessor)
                .writer(classifierCompositeWriter)
                .stream(flatFileItemWriterLarge)
                .stream(flatFileItemWriterMiddle)
                .stream(flatFileItemWriterSmall)
                .build();
    }

    /**
     * Step-3: ItemReader
     * <p>
     * 每一个spring batch job启动时， 都会有一个JobParameter, 使用如下方式获取
     *
     * @Value("#{jobParameters}") Map jobParameters
     */
    @Bean
    @StepScope
    ItemStreamReader<TestUserDTO> dbToMulFileItemReader(DataSource dataSource, @Value("#{jobParameters}") Map jobParameters) {

        JdbcPagingItemReader<TestUserDTO> databaseReader = new JdbcPagingItemReader<>();
        databaseReader.setDataSource(dataSource);
        databaseReader.setPageSize(50);
        databaseReader.setRowMapper(new BeanPropertyRowMapper<>(TestUserDTO.class));
        PagingQueryProvider queryProvider = createQueryProvider(jobParameters);
        databaseReader.setQueryProvider(queryProvider);
        databaseReader.setQueryProvider(queryProvider);
        //logger.error(jobParameters.get("demo").toString());
        //logger.error("memberId:"+(Long)jobParameters.get("memberId"));
        //logger.error("date:"+ (Date)jobParameters.get("currentTime"));
        return databaseReader;
    }

    //Todo 单表的查询，多表的查询怎么办？
    private PagingQueryProvider createQueryProvider(Map jobParameters) {
        MySqlPagingQueryProvider queryProvider = new MySqlPagingQueryProvider();
        SqlPagingQueryProviderFactoryBean sqlPagingQueryProviderFactoryBean = new SqlPagingQueryProviderFactoryBean();

//        queryProvider.setSelectClause("SELECT userid, username, password,country");
        queryProvider.setFromClause("FROM sysuser_bck");
        queryProvider.setSelectClause(" SELECT userid,username,password,country from sysuser union all" +
                " SELECT userid,username,password,country ");

        //可以设置sql的查询条件
        queryProvider.setSortKeys(sortByLastActivityDateDesc());

        return queryProvider;
    }

    private Map<String, Order> sortByLastActivityDateDesc() {
        Map<String, Order> sortConfiguration = new LinkedHashMap<>();

        sortConfiguration.put("userid", Order.ASCENDING);
        return sortConfiguration;
    }
    //Todo 读取多张表数据

    /**
     * Step-3: ItemProcessor
     */
    @Bean
    @StepScope
    ItemProcessor<TestUserDTO, TestUserDTO> dbToMulFileItemProcessor() {
        logger.info("---");
        return new DBToFileProcessor();
    }

    //写到单个文件中
    /*@Bean
    @StepScope
    ItemStreamWriter<TestUserDTO> databaseCsvItemWriter(Environment environment) throws IOException {
        FlatFileItemWriter<TestUserDTO> csvFileWriter = new FlatFileItemWriter<>();


        String exportFileHeader = environment.getRequiredProperty(PROPERTY_CSV_EXPORT_FILE_HEADER);
        StringHeaderWriter headerWriter = new StringHeaderWriter(exportFileHeader);
        csvFileWriter.setHeaderCallback(headerWriter);

        String exportFilePath = environment.getRequiredProperty(PROPERTY_CSV_EXPORT_FILE_PATH);
        csvFileWriter.setResource(new FileSystemResource(exportFilePath));

        LineAggregator<TestUserDTO> lineAggregator = createUserFileLineAggregator();
        csvFileWriter.setLineAggregator(lineAggregator);

        return csvFileWriter;
    }*/


    @Autowired
    UserExportFileRouterClassifier userExportFileRouterClassifier;

    /**
     * Step-3: ItemWriter
     * <p>
     * 分类写，根据不同的内容，写入不同的文件
     */
    @Bean
    @StepScope
    ClassifierCompositeItemWriter<TestUserDTO> classifierCompositeWriter(Environment environment) {

        ClassifierCompositeItemWriter<TestUserDTO> classifierCompositeItemWriter = new ClassifierCompositeItemWriter();
        classifierCompositeItemWriter.setClassifier(backToBackPatternClassifier(environment));

        return classifierCompositeItemWriter;
    }

    @Bean
    BackToBackPatternClassifier backToBackPatternClassifier(Environment environment) {

        BackToBackPatternClassifier backToBackPatternClassifier = new BackToBackPatternClassifier();
        backToBackPatternClassifier.setRouterDelegate(userExportFileRouterClassifier);

        Map matchMap = new HashMap();
        matchMap.put("usa", flatFileItemWriterLarge(environment));
        matchMap.put("china", flatFileItemWriterSmall(environment));
        matchMap.put("others", flatFileItemWriterMiddle(environment));
        backToBackPatternClassifier.setMatcherMap(matchMap);

        return backToBackPatternClassifier;
    }

    /**
     * Step-3: ItemWriter
     * <p>
     * config different writer.
     */
    @Bean
    @StepScope
    FlatFileItemWriter<TestUserDTO> flatFileItemWriterLarge(Environment environment) {

        return produceItemWriter("file-large.csv");
    }

    @Bean
    @StepScope
    FlatFileItemWriter<TestUserDTO> flatFileItemWriterSmall(Environment environment) {

        return produceItemWriter("file-small.csv");
    }

    @Bean
    @StepScope
    FlatFileItemWriter<TestUserDTO> flatFileItemWriterMiddle(Environment environment) {


        return produceItemWriter("file-middle.csv");
    }

    private FlatFileItemWriter<TestUserDTO> produceItemWriter(String fileName) {
        FlatFileItemWriter<TestUserDTO> fileWriter = new FlatFileItemWriter<>();

        //Config file writer header
        String exportFileHeader = SftpConfiguration.getSftpFileHeader();
        StringHeaderWriter headerWriter = new StringHeaderWriter(exportFileHeader);
        fileWriter.setHeaderCallback(headerWriter);

        String exportFilePath = SftpConfiguration.getSftpLocalFileSendPath();
        FileUtil.checkOrCreateFile(exportFilePath+fileName,false);
        fileWriter.setResource(new FileSystemResource(exportFilePath + fileName));

        //设置数据库中，需要提取的字段；设置文件的格式；
        LineAggregator<TestUserDTO> lineAggregator = createUserFileLineAggregator();
        fileWriter.setLineAggregator(lineAggregator);
        return fileWriter;
    }

    private LineAggregator<TestUserDTO> createUserFileLineAggregator() {
        DelimitedLineAggregator<TestUserDTO> lineAggregator = new DelimitedLineAggregator<>();
        lineAggregator.setDelimiter("|");

        FieldExtractor<TestUserDTO> fieldExtractor = createUserFieldExtractor();
        lineAggregator.setFieldExtractor(fieldExtractor);

        return lineAggregator;
    }

    private FieldExtractor<TestUserDTO> createUserFieldExtractor() {
        BeanWrapperFieldExtractor<TestUserDTO> extractor = new BeanWrapperFieldExtractor<>();
        extractor.setNames(new String[]{"userId", "userName", "password", "country"});
        return extractor;
    }

    /*---------------------  Step-4: Synchronize file from local to SFTP server ---------------------*/

    @Resource(name = "toSftpChannel")
    MessageChannel messageChannel;

    /**
     * Step-4: Synchronize file from local to SFTP server
     * <p>
     * send file to SFTP.
     */
    @Bean
    Step sftpFileSendStep(StepBuilderFactory stepBuilderFactory,
                          SftpSendFileTasklet sftpFileSendTasklet) {

        return stepBuilderFactory.get("sftpFileSendStep").tasklet(sftpFileSendTasklet).build();
    }

    /**
     * Send file to SFTP.
     */
    @Bean
    @StepScope
    SftpSendFileTasklet sftpFileSendTasklet(Environment environment) {

        SftpSendFileTasklet sftpTasklet = new SftpSendFileTasklet();
        String exportPath = SftpConfiguration.getSftpLocalFileSendPath();
        //find files matches condition.
        String fileNamePattern = "*.*";
        Path filePath = Paths.get(exportPath);
        SimplePatternFileListFilter filter = new SimplePatternFileListFilter(fileNamePattern);
        List<File> matchingFiles = filter.filterFiles(filePath.toFile().listFiles());
        //Send process
        sftpTasklet.setFileList(matchingFiles);
        sftpTasklet.setSftpChannel(messageChannel);
        return sftpTasklet;
    }
}
