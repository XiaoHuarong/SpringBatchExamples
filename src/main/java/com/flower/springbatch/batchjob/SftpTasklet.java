package com.flower.springbatch.batchjob;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;

import java.io.File;

public class SftpTasklet implements Tasklet {

    private String fileName;
    private MessageChannel sftpChannel;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

        File file = new File(fileName);

        if (file.exists()) {
            Message<File> message = MessageBuilder.withPayload(file).build();
            try {
                sftpChannel.send(message);

            } catch (Exception e) {
                System.out.println("Could not send file per SFTP: " + e);
            }
        } else {
            System.out.println("File does not exist.");
        }

        return RepeatStatus.FINISHED;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public MessageChannel getSftpChannel() {
        return sftpChannel;
    }

    public void setSftpChannel(MessageChannel sftpChannel) {
        this.sftpChannel = sftpChannel;
    }
}