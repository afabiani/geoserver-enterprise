/* Copyright (c) 2001 - 2007 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the GPL 2.0 license, available at the root
 * application directory.
 */
package org.geoserver.wps.mail;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.geoserver.platform.GeoServerExtensions;
import org.vfny.geoserver.global.GeoserverDataDirectory;

import freemarker.template.Configuration;
import freemarker.template.DefaultObjectWrapper;
import freemarker.template.Template;
import freemarker.template.TemplateException;

/**
 * The Class SendMail.
 * 
 * @author "Alessio Fabiani - alessio.fabiani@geo-solutions.it"
 */
public class SendMail {

    /** The props. */
    private Properties props;

    /** The conf. */
    private final MailConfiguration conf = new MailConfiguration();

    /** FreeMarker TEMPLATES *. */
    static final Configuration TEMPLATES;

    static {
        TEMPLATES = new Configuration();
        // same package of this class
        try {
            File templatesPath = getSendMailTemplatesPath();

            if (templatesPath != null) {
                TEMPLATES.setDirectoryForTemplateLoading(templatesPath);
            } else {
                TEMPLATES.setClassForTemplateLoading(SendMail.class, "");
            }
        } catch (IOException e) {
            TEMPLATES.setClassForTemplateLoading(SendMail.class, "");

        }
        TEMPLATES.setObjectWrapper(new DefaultObjectWrapper());
    }

    /**
     * Instantiates a new send mail.
     * 
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public SendMail() throws IOException {
        props = conf.loadConfiguration();
    }

    /**
     * Send an EMail to a specified address.
     * 
     * @param address the to address
     * @param subject the email address
     * @param body message to send
     * @throws MessagingException the messaging exception
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public void send(String address, String subject, String body) throws MessagingException,
            IOException {

        // Session session = Session.getDefaultInstance(props, null);
        Session session = Session.getDefaultInstance(props, (conf.getMailSmtpAuth()
                .equalsIgnoreCase("true") ? new javax.mail.Authenticator() {
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(conf.getUserName(), conf.getPassword());
            }
        } : null));

        try {

            Message message = new MimeMessage(session);
            message.setFrom(new InternetAddress(conf.getFromAddress(), conf.getFromAddressname()));
            message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(address));
            message.setSubject(subject);
            message.setText(body.toString());

            Transport.send(message);

        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * Send a notification to the specified address.
     * 
     * @param toAddress the to address
     * @param executiondId the executiond id
     * @param expirationDelay
     * @throws IOException Signals that an I/O exception has occurred.
     * @throws MessagingException the messaging exception
     */
    public void sendFinishedNotification(String toAddress, String executiondId, String result,
            int expirationDelay) throws IOException, MessagingException {

        // load template for the password reset email
        Template mailTemplate = TEMPLATES.getTemplate("FinishedNotificationMail.ftl");

        StringWriter body = fillMailBody(toAddress, executiondId, result, expirationDelay,
                mailTemplate);

        send(toAddress, conf.getSubjet(), body.toString());
    }

    /**
     * Fill mail body.
     * 
     * @param toAddress the to address
     * @param executiondId the executiond id
     * @param result
     * @param expirationDelay
     * @param mailTemplate the mail template
     * @return the string writer
     * @throws IOException Signals that an I/O exception has occurred.
     */
    private StringWriter fillMailBody(String toAddress, String executiondId, String result,
            int expirationDelay, Template mailTemplate) throws IOException {

        // create template context
        StringWriter body = new StringWriter();
        Map<String, Object> templateContext = new HashMap<String, Object>();
        templateContext.put("toAddress", toAddress);
        templateContext.put("executiondId", executiondId);

        String millis = String.format(
                "%d min, %d sec",
                TimeUnit.MILLISECONDS.toMinutes(expirationDelay),
                TimeUnit.MILLISECONDS.toSeconds(expirationDelay)
                        - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS
                                .toMinutes(expirationDelay)));

        if (expirationDelay > 0) {
            templateContext.put("expirationDelay", millis);
        }
        if (result != null) {
            templateContext.put("result", result.toString());
        }

        // create message string
        try {
            mailTemplate.process(templateContext, body);
        } catch (TemplateException e) {
            throw new IOException(e);

        }
        return body;
    }

    /**
     * Send started notification.
     * 
     * @param toAddress the to address
     * @param executiondId the executiond id
     * @throws IOException Signals that an I/O exception has occurred.
     * @throws MessagingException the messaging exception
     */
    public void sendStartedNotification(String toAddress, String executiondId) throws IOException,
            MessagingException {
        // load template for the password reset email
        Template mailTemplate = TEMPLATES.getTemplate("StartedNotificationMail.ftl");

        StringWriter body = fillMailBody(toAddress, executiondId, null, 0, mailTemplate);

        send(toAddress, conf.getSubjet(), body.toString());
    }

    /**
     * Send started notification.
     * 
     * @param toAddress the to address
     * @param executiondId the executiond id
     * @throws IOException Signals that an I/O exception has occurred.
     * @throws MessagingException the messaging exception
     */
    public void sendFailedNotification(String toAddress, String executiondId, String reason)
            throws IOException {
        // load template for failed error
        Template mailTemplate = TEMPLATES.getTemplate("FailedNotificationMail.ftl");

        // create template context
        StringWriter body = new StringWriter();
        Map<String, Object> templateContext = new HashMap<String, Object>();
        templateContext.put("toAddress", toAddress);
        templateContext.put("executiondId", executiondId);
        templateContext.put("reason", reason);

        // create message string
        try {
            mailTemplate.process(templateContext, body);
            send(toAddress, conf.getSubjet(), body.toString());
        } catch (Exception e) {
            throw new IOException(e);
        }

    }

    /**
     * 
     * @return
     * @throws IOException
     */
    public static File getSendMailTemplatesPath() throws IOException {
        // get the temporary storage for WPS
        try {
            File storage = GeoserverDataDirectory.findCreateConfigDir("wps-cluster/templates");
            return storage;
        } catch (Exception e) {
            throw new IOException("Could not find the data directory for WPS CLUSTER");
        }

    }
}
