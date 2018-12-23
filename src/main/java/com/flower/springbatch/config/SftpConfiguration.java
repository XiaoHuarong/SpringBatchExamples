package com.flower.springbatch.config;

import com.jcraft.jsch.ChannelSftp.LsEntry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.io.Resource;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.annotation.Gateway;
import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.file.DefaultFileNameGenerator;
import org.springframework.integration.file.FileNameGenerator;
import org.springframework.integration.file.remote.session.CachingSessionFactory;
import org.springframework.integration.file.remote.session.SessionFactory;
import org.springframework.integration.sftp.filters.SftpSimplePatternFileListFilter;
import org.springframework.integration.sftp.inbound.SftpInboundFileSynchronizer;
import org.springframework.integration.sftp.outbound.SftpMessageHandler;
import org.springframework.integration.sftp.session.DefaultSftpSessionFactory;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;

import java.io.File;
import java.util.Properties;


/**
 * Configuration for SFTP.
 */
@Configuration
public class SftpConfiguration {

    @Value("${sftp.host}")
    private String sftpHost;

    @Value("${sftp.port:23}")
    private int sftpPort;

    @Value("${sftp.user}")
    private String sftpUser;

    @Value("${sftp.privateKey:#{null}}")
    private Resource sftpPrivateKey;

    @Value("${sftp.privateKeyPassphrase:}")
    private String sftpPrivateKeyPassphrase;

    @Value("${sftp.password}")
    private String sftpPassword;

    @Bean
    public SessionFactory<LsEntry> sftpSessionFactory() {
        DefaultSftpSessionFactory factory = new DefaultSftpSessionFactory(true);
        factory.setHost(sftpHost);
        factory.setPort(sftpPort);
        factory.setUser(sftpUser);
        Properties jschProps = new Properties();
        //!important 必须配置PreferredAuthentications，否则程序控制台会询问user name 和 password。
        jschProps.put("StrictHostKeyChecking", "no");
        jschProps.put("PreferredAuthentications",
            "password,gssapi-with-mic,publickey,keyboard-interactive");

        factory.setSessionConfig(jschProps);

//      factory.setSessionConfig();

        /*if (sftpPrivateKey != null) {
            factory.setPrivateKey(sftpPrivateKey);
            factory.setPrivateKeyPassphrase(sftpPrivateKeyPassphrase);
        } else {
            factory.setPassword(sftpPassword);
        }*/
        if (sftpPassword != null){
            factory.setPassword(sftpPassword);
        } else {
            factory.setPrivateKey(sftpPrivateKey);
            factory.setPrivateKeyPassphrase(sftpPrivateKeyPassphrase);
        }

        factory.setAllowUnknownKeys(true);
        return new CachingSessionFactory<LsEntry>(factory);
    }
    ////////////////////////////////////////////////////////////////////////

    @MessagingGateway
    public interface UploadGateway {
        @Gateway(requestChannel = "toSftpChannel")
        void upload(File file);
    }

    private static String sftpRemoteDirectorySend;

    private static String sftpRemoteDirectoryGet;

    private static String sftpLocalDirectorySend;

    private static String sftpLocalDirectoryGet;

    private static String sftpRemoteFileFilter;


    /**
     * Define SFTP Inbound bean. Synchronize file to local server from SFTP.
     * Wired in other beans to synchronize file, such as "SftpGetFileTasklet".
     *
     * @return Sftp Inbound File Synchronize
     */
    @Bean(name = "sftpInbound")
    public SftpInboundFileSynchronizer sftpInboundFileSynchronizer() {
        SftpInboundFileSynchronizer fileSynchronize = new SftpInboundFileSynchronizer(sftpSessionFactory());
        fileSynchronize.setDeleteRemoteFiles(false);
        //!important
        fileSynchronize.setRemoteDirectory(sftpRemoteDirectorySend);
        fileSynchronize.setFilter(new SftpSimplePatternFileListFilter(sftpRemoteFileFilter));
        return fileSynchronize;
    }

    /**
     * Send file to SFTP server from local.
     * Monitor what has been done to remote SFTP server via "toSftpChannel" channel.
     *
     * @return
     */
    @Bean
    @ServiceActivator(inputChannel = "toSftpChannel")
    public MessageHandler handler() {
        SftpMessageHandler handler = new SftpMessageHandler(sftpSessionFactory());
        //!important
        handler.setRemoteDirectoryExpression(new LiteralExpression(sftpRemoteDirectoryGet));
        handler.setAutoCreateDirectory(true);
        /*handler.setFileNameGenerator(new FileNameGenerator() {
            @Override
            public String generateFileName(Message<?> message) {
                if (message.getPayload() instanceof File) {
                    return ((File) message.getPayload()).getName();
                } else {
                    throw new IllegalArgumentException("File expected as payload.");
                }
            }
        });*/
        handler.setCharset("UTF-8");
        handler.setFileNameGenerator(fileNameGenerator());
        return handler;
    }
    /**
     * File name generator.
     * <p>
     * File name is same as in local server, after upload to SFTP server.
     */
    @Bean
    DefaultFileNameGenerator fileNameGenerator() {
        return new DefaultFileNameGenerator();
    }


    private static String sftpFileSuffix;


    private static String sftpFileHeader;




    @Value("${sftp.file.path.remote.send}")
    public void setSftpRemoteDirectorySend(String sftpRemoteDirectorySend) {
        SftpConfiguration.sftpRemoteDirectorySend = sftpRemoteDirectorySend;
    }

    @Value("${sftp.file.path.remote.get}")
    public void setSftpRemoteDirectoryGet(String sftpRemoteDirectoryGet) {
        SftpConfiguration.sftpRemoteDirectoryGet = sftpRemoteDirectoryGet;
    }

    @Value("${sftp.file.path.local.get}")
    public void setSftpLocalDirectorySend(String sftpLocalDirectorySend) {
        SftpConfiguration.sftpLocalDirectorySend = sftpLocalDirectorySend;
    }

    @Value("${sftp.file.path.local.get}")
    public void setSftpLocalDirectoryGet(String sftpLocalDirectoryGet) {
        SftpConfiguration.sftpLocalDirectoryGet = sftpLocalDirectoryGet;
    }

    @Value("${sftp.remote.file.filter:*.*}")
    public void setSftpRemoteFileFilter(String sftpRemoteFileFilter) {
        SftpConfiguration.sftpRemoteFileFilter = sftpRemoteFileFilter;
    }

    @Value("${sftp.file.suffix}")
    public void setSftpFileSuffix(String sftpFileSuffix) {
        SftpConfiguration.sftpFileSuffix = sftpFileSuffix;
    }

    @Value("${sftp.file.header}")
    public void setSftpFileHeader(String sftpFileHeader) {
        SftpConfiguration.sftpFileHeader = sftpFileHeader;
    }

    public static String getSftpRemoteFileSendPath(){

        return sftpRemoteDirectorySend;
    }

    public static String getSftpFileSuffix(){

        return sftpFileSuffix;
    }

    public static String getSftpLocalFileSendPath(){

        return sftpLocalDirectorySend;
    }

    public static String getSftpLocalFileGetPath(){

        return sftpLocalDirectoryGet;
    }

    public static String getSftpFileHeader(){

        return sftpFileHeader;
    }

    public static String getSftpRemoteFileFilter(){

        return sftpRemoteFileFilter;
    }
}
