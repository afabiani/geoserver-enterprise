/* Copyright (c) 2001 - 2007 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the GPL 2.0 license, availible at the root
 * application directory.
 */
package org.geoserver.wps.mail;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import freemarker.template.Configuration;
import freemarker.template.DefaultObjectWrapper;
import freemarker.template.Template;
import freemarker.template.TemplateException;

/**
 * The Class SendMail.
 * 
 * @author frank
 */
public class SendMail {

    /** The props. */
    private Properties props;

    final MailConfiguration conf = new MailConfiguration();

    /** FreeMarker templates **/
    static final Configuration templates;

    static {

        templates = new Configuration();
        // same package of this class
        templates.setClassForTemplateLoading(SendMail.class, "");
        templates.setObjectWrapper(new DefaultObjectWrapper());
    }

    /**
     * Instantiates a new send mail.
     * 
     * @throws IOException
     */
    public SendMail() throws IOException {
        props = conf.loadConfiguration();
    }

    /**
     * Send an EMail to a specified address.
     * 
     * @param address the to address
     * 
     * @param subject the email address
     * 
     * @param body message to send
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

            // session.getTransport("smtp").send(message);

            Transport.send(message);

        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        } finally {

        }

    }

    /**
     * Send a notification to the specified address
     * 
     * @param toAddress
     * @throws IOException
     */
    public void sendFinishedNotification(String toAddress, String executiondId) throws IOException,
            MessagingException {

        // load template for the password reset email
        Template mailTemplate = templates.getTemplate("NotificationMail.ftl");

        StringWriter body = new StringWriter();

        if (mailTemplate != null) {
            // create template context
            Map<String, Object> templateContext = new HashMap<String, Object>();
            templateContext.put("toAddress", toAddress);
            templateContext.put("executiondId", executiondId);
            // create message string
            try {
                mailTemplate.process(templateContext, body);
            } catch (TemplateException e) {
                e.printStackTrace();

            }
        } else {
            body.append(conf.getBody());
        }

        send(toAddress, conf.getSubjet(), body.toString());
    }

}
