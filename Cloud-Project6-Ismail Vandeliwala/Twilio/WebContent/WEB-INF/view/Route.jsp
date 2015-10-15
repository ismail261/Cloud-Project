<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<%@ taglib uri="http://www.springframework.org/tags/form" prefix="form"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
  <head>
    <meta charset="UTF-8" />
    <title>Find a route</title>
    <script src="http://maps.google.com/maps/api/js?sensor=true"></script>
    <script src="http://ajax.googleapis.com/ajax/libs/jquery/1.8.3/jquery.min.js"></script>
    <script>
      function calculateRoute(from, to) {
        // Center initialized to Naples, Italy
        var myOptions = {
          zoom: 10,
          center: new google.maps.LatLng(40.84, 14.25),
          mapTypeId: google.maps.MapTypeId.ROADMAP
        };
        // Draw the map
        var mapObject = new google.maps.Map(document.getElementById("map"), myOptions);
 
        var directionsService = new google.maps.DirectionsService();
        var directionsRequest = {
          origin: from,
          destination: to,
          travelMode: google.maps.DirectionsTravelMode.DRIVING,
          unitSystem: google.maps.UnitSystem.METRIC
        };
        directionsService.route(
          directionsRequest,
          function(response, status)
          {
            if (status == google.maps.DirectionsStatus.OK)
            {
              new google.maps.DirectionsRenderer({
                map: mapObject,
                directions: response
              });
            }
            else
              $("#error").append("Unable to retrieve your route<br />");
          }
        );
      }
 
      $(document).ready(function() {
        // If the browser supports the Geolocation API
        if (typeof navigator.geolocation == "undefined") {
          $("#error").text("Your browser doesn't support the Geolocation API");
          return;
        }
 
        $("#from-link, #to-link").click(function(event) {
          event.preventDefault();
          var addressId = this.id.substring(0, this.id.indexOf("-"));
 
          navigator.geolocation.getCurrentPosition(function(position) {
            var geocoder = new google.maps.Geocoder();
            geocoder.geocode({
              "location": new google.maps.LatLng(position.coords.latitude, position.coords.longitude)
            },
            function(results, status) {
              if (status == google.maps.GeocoderStatus.OK)
                $("#" + addressId).val(results[0].formatted_address);
              else
                $("#error").append("Unable to retrieve your address<br />");
            });
          },
          function(positionError){
            $("#error").append("Error: " + positionError.message + "<br />");
          },
          {
            enableHighAccuracy: true,
            timeout: 10 * 1000 // 10 seconds
          });
        });
 
        $("#calculate-route").submit(function(event) {
          event.preventDefault();
          calculateRoute($("#from").val(), $("#to").val());
        });
      });
    </script>
    <style type="text/css">
      #map {
        width: 500px;
        height: 400px;
        margin-top: 10px;
      }
    </style>
  </head>
  <body>
  <table width="100%">
	<tr>
		<td><center><h1>Find Route between 2 places: </h1></center></td>
	</tr>
	</table>
    <form id="calculate-route" name="calculate-route" action="#" method="get">
    <table width="100%">
    	<tr>
			<td align="right" width="50%">From: </td>
			<td align="left" width="50%">
				<input type="text" name="from" id="from" required="required" placeholder="Current Address"/>
				<a id="from-link" href="#">Get my position</a>
			</td>
		</tr>
		<tr>
			<td align="right" width="50%">To: </td>
			<td align="left" width="50%">
				<input type="text" name="to" id="to" required="required" placeholder="Destination Address"/>
				<a id="to-link" href="#">Get my position</a>
			</td>
		</tr>
		<tr>
			<td align="right" width="50%"><input type="submit" /></td>
			<td align="left" width="50%"><input type="reset" /></td>
		</tr>
     </table>
    </form>
    <center>
    	<div id="map"></div>
    </center>
    <p id="error"></p>
  </body>
</html>