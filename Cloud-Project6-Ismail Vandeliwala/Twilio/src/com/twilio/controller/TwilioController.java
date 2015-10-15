package com.twilio.controller;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.servlet.ModelAndView;

import com.twilio.sdk.TwilioRestException;
import com.twilio.sdk.client.TwilioCapability;
import com.twilio.sdk.client.TwilioCapability.DomainException;

@Controller
public class TwilioController {

	 public static final String ACCOUNT_SID = "AC025e38604d25f7c966f7a55679c2bbb5";
	 public static final String AUTH_TOKEN = "2bd0d2d4ec4cb6fc3bb52ee25258411b";
	 public static final String sid = "AP7c2db3cb6ad51897a2a0ce87bf090219";
	    
	@RequestMapping("/homePage")
	public ModelAndView getHomePage(){
		
		ModelMap modelMap = new ModelMap();
		return new ModelAndView("HomePage", modelMap);
	}
	
	@RequestMapping("/sms")
	public ModelAndView sms(){
		
		ModelMap modelMap = new ModelMap();
		return new ModelAndView("Sms", modelMap);
	}
	
	@RequestMapping(value="/incomingVoice")
	public ModelAndView incomingVoice(){
		
		ModelMap modelMap = new ModelMap();
		return new ModelAndView("IncomingVoice", modelMap);
	}
	
	@RequestMapping(value="/voice")
	public ModelAndView voice(){
		
		ModelMap modelMap = new ModelMap();
		return new ModelAndView("Voice", modelMap);
	}
	
	@RequestMapping(value="/route")
	public ModelAndView route(){
		
		ModelMap modelMap = new ModelMap();
		return new ModelAndView("Route", modelMap);
	}
	
	@RequestMapping("/sendSmsMessage")
	public ModelAndView sendSmsMessage(HttpServletRequest request) throws TwilioRestException {
		
		ModelMap modelMap = new ModelMap();
		SmsSender.sendMessage(request.getParameter("number"),request.getParameter("msg"));
		return new ModelAndView("HomePage", modelMap);
	}
	
	@RequestMapping(value="/sendVoiceMessage",method=RequestMethod.POST)
	public ModelAndView sendVoiceMessage(HttpServletRequest request) throws TwilioRestException {
		
		ModelMap modelMap = new ModelMap();
		VoiceSender.sendVoiceMessage(request.getParameter("number"),request.getParameter("voiceMsg"));
		return new ModelAndView("HomePage", modelMap);
	}
	
	@RequestMapping(value="/listenIncomingMessage",method=RequestMethod.POST)
	public ModelAndView listenIncomingMessage(HttpServletRequest request, HttpServletResponse response) throws TwilioRestException, ServletException, IOException {
		
		ModelMap modelMap = new ModelMap();
		 String applicationSid = sid;
		 
	        TwilioCapability capability = new TwilioCapability(ACCOUNT_SID, AUTH_TOKEN);
	        capability.allowClientOutgoing(applicationSid);
	 
	        String token = null;
	        try {
	            token = capability.generateToken();
	        } catch (DomainException e) {
	            e.printStackTrace();
	        }
	        // Forward the token information to a JSP view
	        response.setContentType("text/xml");
	        request.setAttribute("token", token);
	        
		return new ModelAndView("client", modelMap);
	}
}
