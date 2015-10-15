package com.twilio.controller;

import com.twilio.sdk.resource.instance.Account;
import com.twilio.sdk.TwilioRestClient;
import com.twilio.sdk.TwilioRestException;
import com.twilio.sdk.resource.factory.MessageFactory;
import com.twilio.sdk.resource.instance.Message;
import java.util.ArrayList;
import java.util.List;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;
 
public class SmsSender {
 
    /* Find your sid and token at twilio.com/user/account */
    public static final String ACCOUNT_SID = "AC025e38604d25f7c966f7a55679c2bbb5";
    public static final String AUTH_TOKEN = "2bd0d2d4ec4cb6fc3bb52ee25258411b";
 
    public static void sendMessage(String number, String msg) throws TwilioRestException{
    	TwilioRestClient client = new TwilioRestClient(ACCOUNT_SID, AUTH_TOKEN);
    	 
        Account account = client.getAccount();
 
        MessageFactory messageFactory = account.getMessageFactory();
        List<NameValuePair> params = new ArrayList<NameValuePair>();
        params.add(new BasicNameValuePair("To", "+1"+number)); // Replace with a valid phone number for your account.
        params.add(new BasicNameValuePair("From", "+14693514894")); // Replace with a valid phone number for your account.
        params.add(new BasicNameValuePair("Body", msg));
        Message sms = messageFactory.create(params);
    }
}
