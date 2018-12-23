package com.flower.springbatch.batchjob.springintegration.sftp;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.integration.file.filters.SimplePatternFileListFilter;
import org.springframework.integration.sftp.inbound.SftpInboundFileSynchronizer;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;

/**
 * Spring-batch-tasklet: Achieve file from SFTP server.
 */
public class SftpGetFileTasklet implements Tasklet, InitializingBean {

    private File localDirectory;

    private boolean autoCreateLocalDirectory = true;

    private boolean deleteLocalFiles = true;

    private String fileNamePattern;

    private String remoteDirectory;

    private int downloadFileAttempts = 12;

    private long retryIntervalMilliseconds = 300000;

    private boolean retryIfNotFound = false;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private SftpInboundFileSynchronizer ftpInboundFileSynchronizer;

    //Create local file folder.
    @Override
    public void afterPropertiesSet() throws Exception {

        if (!this.localDirectory.exists()) {
            if (this.autoCreateLocalDirectory) {
                this.localDirectory.mkdirs();
            } else {
                throw new FileNotFoundException(this.localDirectory.getName());
            }
        }
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        //If exist,delete it, in order to send the latest version.
        deleteLocalFiles();
        ftpInboundFileSynchronizer.synchronizeToLocalDirectory(localDirectory);
        //Set retry counts to achieve file.
        if (retryIfNotFound) {
            SimplePatternFileListFilter filter = new SimplePatternFileListFilter(fileNamePattern);
            int attemptCount = 1;
            while (filter.filterFiles(localDirectory.listFiles()).size() == 0 && attemptCount <= downloadFileAttempts) {
                logger.info("File(s) matching " + fileNamePattern + " not found on remote SFTP.  Attempt " + attemptCount + " out of " + downloadFileAttempts);
                Thread.sleep(retryIntervalMilliseconds);
                ftpInboundFileSynchronizer.synchronizeToLocalDirectory(localDirectory);
                attemptCount++;
            }
            if (attemptCount >= downloadFileAttempts && filter.filterFiles(localDirectory.listFiles()).size() == 0) {

                throw new FileNotFoundException("No found remote file(s) matching:" + fileNamePattern);
            }
        }
        return null;
    }

    //delete local files.
    private void deleteLocalFiles() {
        if (deleteLocalFiles) {
            SimplePatternFileListFilter filter = new SimplePatternFileListFilter(fileNamePattern);
            List<File> matchingFiles = filter.filterFiles(localDirectory.listFiles());
            if (matchingFiles.size() > 0) {
                for (File file : matchingFiles) {
                    FileUtils.deleteQuietly(file);
                }
            }
        }
    }


    public void setLocalDirectory(File localDirectory) {
        this.localDirectory = localDirectory;
    }


    public void setFtpInboundFileSynchronizer(SftpInboundFileSynchronizer ftpInboundFileSynchronizer) {
        this.ftpInboundFileSynchronizer = ftpInboundFileSynchronizer;
    }

    public void setFileNamePattern(String fileNamePattern) {
        this.fileNamePattern = fileNamePattern;
    }

    public void setRemoteDirectory(String remoteDirectory) {
        this.remoteDirectory = remoteDirectory;
    }

    public void setDownloadFileAttempts(int downloadFileAttempts) {
        this.downloadFileAttempts = downloadFileAttempts;
    }

    public void setRetryIntervalMilliseconds(long retryIntervalMilliseconds) {
        this.retryIntervalMilliseconds = retryIntervalMilliseconds;
    }

    public void setRetryIfNotFound(boolean retryIfNotFound) {
        this.retryIfNotFound = retryIfNotFound;
    }
}
