<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<%@ taglib uri="http://www.springframework.org/tags/form" prefix="form"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>Twilio</title>
</head>
<body>
<center>
<form:form id="twilioForm" method="post" action="listenIncomingMessage.html">
<table width="100%">
	<tr>
		<td><center><h1>Via Incoming Voice: </h1></center></td>
	</tr>
</table>
<table width="100%">
	<tr>
		<td align="center">
			<input type="submit" value="Click !!" class="button"/>
		</td>
	</tr>
</table>
</form:form>
</center>
</body>
</html>