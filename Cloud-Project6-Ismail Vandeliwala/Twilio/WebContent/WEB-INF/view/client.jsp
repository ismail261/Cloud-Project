<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
  <head>
    <title>Hello Client Monkey 1</title>
    <script type="text/javascript"
      src="//static.twilio.com/libs/twiliojs/1.2/twilio.min.js"></script>
    <script type="text/javascript"
      src="https://ajax.googleapis.com/ajax/libs/jquery/1.7.2/jquery.min.js">
    </script>
    <link href="//static0.twilio.com/packages/quickstart/client.css"
      type="text/css" rel="stylesheet" />
    <script type="text/javascript">
  
    /* Create the Client with a Capability Token */
    Twilio.Device.setup("${token}", {debug: true});
 
    /* Let us know when the client is ready. */
    Twilio.Device.ready(function (device) {
        $("#log").text("Ready");
    });
 
    /* Report any errors on the screen */
    Twilio.Device.error(function (error) {
        $("#log").text("Error: " + error.message);
    });
 
    Twilio.Device.connect(function (conn) {
        $("#log").text("Successfully established call");
    });
 
    /* Connect to Twilio when we call this function. */
    function call() {
        Twilio.Device.connect();
    }
    </script>
  </head>
  <body>
 
    <button class="call" onclick="call();">
      Call
    </button>
  
    <div id="log">Loading pigeons...</div>
  </body>
</html>