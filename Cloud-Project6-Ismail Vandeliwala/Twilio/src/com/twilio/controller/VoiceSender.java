package com.twilio.controller;

import java.util.Map;
import java.util.HashMap;
 
import com.twilio.sdk.TwilioRestClient;
import com.twilio.sdk.TwilioRestException;
import com.twilio.sdk.resource.instance.Account;
import com.twilio.sdk.resource.instance.Call;
import com.twilio.sdk.resource.factory.CallFactory;
 
public class VoiceSender {
 
	public static final String ACCOUNT_SID = "AC025e38604d25f7c966f7a55679c2bbb5";
    public static final String AUTH_TOKEN = "2bd0d2d4ec4cb6fc3bb52ee25258411b";
    public static final String sid = "AP7c2db3cb6ad51897a2a0ce87bf090219";
 
    public static void sendVoiceMessage(String number, String voiceMsg) throws TwilioRestException {
 
        TwilioRestClient client = new TwilioRestClient(ACCOUNT_SID, AUTH_TOKEN);
        Account mainAccount = client.getAccount();
        CallFactory callFactory = mainAccount.getCallFactory();
        Map<String, String> callParams = new HashMap<String, String>();
        callParams.put("To", "+1"+number); // Replace with your phone number
        callParams.put("From", "+14693514894"); // Replace with a Twilio number
        String Url = "http://twimlets.com/voicemail?Email=ismail.vandeliwala%40gmail.com&Transcribe=true&Message=";
        voiceMsg = voiceMsg.replace(" ", "%20");
        Url = Url + voiceMsg;
        callParams.put("Url", Url);
        // Make the call
        Call call = callFactory.create(callParams);
    }
}
		
