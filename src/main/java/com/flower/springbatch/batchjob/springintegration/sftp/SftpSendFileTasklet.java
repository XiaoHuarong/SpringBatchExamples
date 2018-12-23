package com.flower.springbatch.batchjob.springintegration.sftp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Spring-batch-tasklet: Send file to SFTP server from local.
 */
public class SftpSendFileTasklet implements Tasklet {

    private List<File> fileList;
    private MessageChannel sftpChannel;

    private static final Logger log = LoggerFactory.getLogger(SftpSendFileTasklet.class);

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

//        File file = new File(fileName);

        List<Message> dd = new ArrayList<>();
        /*if (file.exists()) {
            Message<File> message = MessageBuilder.withPayload(file).build();

            try {
                sftpChannel.send(message);
                log.info("Send file to SFTP success." + fileName);
            } catch (Exception e) {
                log.error("Could not send file to SFTP: " + fileName + ". Reason:" + e.getMessage());
            }
        } else {
            log.error("File does not exist {}:", fileName);
        }*/
        if (fileList.size() > 0) {
            for (File file : fileList) {
                Message<File> message = MessageBuilder.withPayload(file).build();

                try {
                    sftpChannel.send(message);
                    log.info("Send files to SFTP success.");
                } catch (Exception e) {
                    log.error("Could not send files to SFTP " + e.getMessage());
                }
            }
        }


        return RepeatStatus.FINISHED;
    }

    public List<File> getFileList() {
        return fileList;
    }

    public void setFileList(List<File> fileList) {
        this.fileList = fileList;
    }

    public MessageChannel getSftpChannel() {
        return sftpChannel;
    }

    public void setSftpChannel(MessageChannel sftpChannel) {
        this.sftpChannel = sftpChannel;
    }
}
