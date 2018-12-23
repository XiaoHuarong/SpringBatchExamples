package com.flower.springbatch.batchjob.util;

import java.io.File;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Vector;



import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Providing utils to get files from SFTP server, or put files to SFTP server.
 */
@Component
public class SftpClientUtil {


    private static String sftpHost;


    private static int sftpPort;


    private static String sftpUser;

/*    @Value("${batchjob.privateKey:#{null}}")
    private Resource sftpPrivateKey;

    @Value("${batchjob.privateKeyPassphrase:}")
    private String sftpPrivateKeyPassphrase;*/


    private static String sftpPassword;

   /* @Value("${batchjob.remote.directory:/}")
    private String sftpRemoteDirectory;*/

    protected static Logger logger = LoggerFactory.getLogger(SftpClientUtil.class);

    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyMM");

    private static String CUSTOMER_POINTS_FILE_PATH = "/in";
    private static String CUSTOMER_POINTS_FILE_NAME_PREFIX = "user-password-";
    private static String CUSTOMER_POINTS_FILE_NAME_SUFFIX = ".csv";


    /*@Value("${batchjob.host}")
    public void setSvnUrl(String svnUrl) {
        SVN_URL = svnUrl;
    }*/


    public SftpClientUtil(@Value("${sftp.host}") String sftpHost, @Value("${sftp.port:23}") int sftpPort,
                          @Value("${sftp.user}") String sftpUser, @Value("${sftp.password}") String sftpPassword){
        this.sftpHost = sftpHost;
        this.sftpPort = sftpPort;
        this.sftpUser = sftpUser;
        this.sftpPassword = sftpPassword;
    }

    private static JSch jsch = new JSch();

    /**
     *
     * @param
     * @param
     * @param
     * @param
     * @return
     * @throws JSchException
     */
    /*private static Session getSession(String user, String password, String address, int port) throws JSchException {
        Session session = jsch.getSession(user, address, port);
        session.setConfig("StrictHostKeyChecking", "no");
        session.setPassword(password);
        session.connect();
        return session;
    }*/

    private static Session getSession() throws JSchException {
        Session session = jsch.getSession(sftpUser, sftpHost, sftpPort);
        session.setConfig("StrictHostKeyChecking", "no");
        session.setConfig("PreferredAuthentications", "publickey,keyboard-interactive,password");//关闭 Kerberos 每一次连接都验证登录名和密码；
        session.setPassword(sftpPassword);
        session.connect();
        return session;
    }
    //    @Bean
//    public SessionFactory<ChannelSftp.LsEntry> sftpSessionFactory() {
//        DefaultSftpSessionFactory factory = new DefaultSftpSessionFactory(true);
//        factory.setHost(sftpHost);
//        factory.setPort(sftpPort);
//        factory.setUser(sftpUser);
//        /*if (sftpPrivateKey != null) {
//            factory.setPrivateKey(sftpPrivateKey);
//            factory.setPrivateKeyPassphrase(sftpPrivateKeyPassphrase);
//        } else {
//            factory.setPassword(sftpPassword);
//        }*/
//        if (sftpPassword != null){
//            factory.setPassword(sftpPassword);
//        } else {
//            factory.setPrivateKey(sftpPrivateKey);
//            factory.setPrivateKeyPassphrase(sftpPrivateKeyPassphrase);
//        }
//
//        factory.setAllowUnknownKeys(true);
//        return new CachingSessionFactory<LsEntry>(factory);
//    }


    public static Vector<LsEntry> getRemoteFileList() {
        try {
            return getRemoteFileList(".");
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static Vector<LsEntry> getRemoteFileList(String cwd) throws JSchException,
        SftpException, Exception {

        Session session = getSession();
        Vector<LsEntry> lsVec=null;
        Channel channel = session.openChannel("batchjob");
        channel.connect();
        ChannelSftp sftpChannel = (ChannelSftp) channel;
        try {
            lsVec=(Vector<LsEntry>)sftpChannel.ls(cwd);

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            sftpChannel.exit();
            channel.disconnect();
            session.disconnect();
        }
        return lsVec;
    }

    public static byte[] getRemoteFile(){


        Vector<LsEntry> lsVec=null;
        InputStream file = null;
        Session session = null;
        ChannelSftp sftpChannel = null;
        Channel channel = null;
        byte [] content =  new byte[1024];
        try {
            session = getSession();
        channel = session.openChannel("batchjob");
        channel.connect();
         sftpChannel = (ChannelSftp) channel;
//            lsVec=(Vector<LsEntry>)sftpChannel.ls(cwd);
//            sftpChannel.cd("/pg-customer-syn-batchjob/user-points");
            sftpChannel.cd(CUSTOMER_POINTS_FILE_PATH);
            //
            Calendar today = Calendar.getInstance();
            today.set(Calendar.MONTH,today.get(Calendar.MONTH) - 1);
            String fileMiddleName = sdf.format(today.getTime());
            file =  sftpChannel.get(CUSTOMER_POINTS_FILE_NAME_PREFIX + fileMiddleName + CUSTOMER_POINTS_FILE_NAME_SUFFIX);

            int filePath = file.available();
            if (filePath > 1024){
                content = new byte[filePath];
            }
            IOUtils.read(file, content);
        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            sftpChannel.exit();
            channel.disconnect();
            session.disconnect();
        }
        return content;
    }


    public static void createFile(InputStream inputStream){


        Session session = null;
        ChannelSftp sftpChannel = null;
        Channel channel = null;
        File file = null;
        byte [] content =  new byte[1024];
        try {
            session = getSession();
            channel = session.openChannel("batchjob");
            channel.connect();
            sftpChannel = (ChannelSftp) channel;
//            lsVec=(Vector<LsEntry>)sftpChannel.ls(cwd);
//            sftpChannel.cd("/pg-customer-syn-batchjob/user-points");
            sftpChannel.cd(CUSTOMER_POINTS_FILE_PATH);



            sftpChannel.mkdir("/file");
            File files = new File("/file");
            files.createNewFile();

            sftpChannel.setInputStream(inputStream);
        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            sftpChannel.exit();
            channel.disconnect();
            session.disconnect();
        }
    }

}
