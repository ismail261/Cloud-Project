#!/usr/bin/python

import httplib2
import pprint
import time

from apiclient.discovery import build
from apiclient.http import MediaFileUpload
from oauth2client.client import OAuth2WebServerFlow


# Copy your credentials from the console
CLIENT_ID = '135248680417-jvna7sa41ae8vbfq5kgqb6q5ubfovkj9.apps.googleusercontent.com'
CLIENT_SECRET = 'NZja3IU0VgpljFal_LrapMo7'
#CLIENT_ID = '418865297255-s1i8272rntvgnq72abatg08eqqtpkpep.apps.googleusercontent.com'
#CLIENT_SECRET = '3A2cpzIEUf6SSL9RUPwp7x6O'

# Check https://developers.google.com/drive/scopes for all available scopes
OAUTH_SCOPE = 'https://www.googleapis.com/auth/drive'

# Redirect URI for installed apps
REDIRECT_URI = 'urn:ietf:wg:oauth:2.0:oob'

# Run through the OAuth flow and retrieve credentials
flow = OAuth2WebServerFlow(CLIENT_ID, CLIENT_SECRET, OAUTH_SCOPE,
                           redirect_uri=REDIRECT_URI)
authorize_url = flow.step1_get_authorize_url()
print('Go to the following link in your browser: ' + authorize_url)
code = raw_input('Enter verification code: ').strip()
credentials = flow.step2_exchange(code)

# Create an httplib2.Http object and authorize it with our credentials
http = httplib2.Http()
http = credentials.authorize(http)

drive_service = build('drive', 'v2', http=http)

# Path to the file to upload - uploading csv file with 10000 records and measuring time
FILENAME = 'D:\\books\\semester4\\Cloud_Computing\\Project5-GWE\\10K.csv'

# Insert a file
media_body = MediaFileUpload(FILENAME, mimetype='text/csv', resumable=True)
body = {
  'title': '10K',
  'description': 'A test document',
  'mimeType': 'text/csv'
}

start = time.clock()
file = drive_service.files().insert(body=body, media_body=media_body).execute()
end = time.clock()
print ("uploading 10K: "+ str(end-start))

downloadUrl = file.get('downloadUrl')
# downloading csv file with 10000 records and measuring time
start = time.clock()
resp, content = drive_service._http.request(downloadUrl)
if resp.status == 200:
    outstream = open('10K-download.csv', 'wb')
    outstream.write(content)
    outstream.close()
    end = time.clock()
    print ("downloading 10K: "+ str(end-start))

# Path to the file to upload - uploading csv file with 25000 records and measuring time
FILENAME = 'D:\\books\\semester4\\Cloud_Computing\\Project5-GWE\\25K.csv'

# Insert a file
media_body = MediaFileUpload(FILENAME, mimetype='text/csv', resumable=True)
body = {
  'title': '25K',
  'description': 'A test document',
  'mimeType': 'text/csv'
}

start = time.clock()
file = drive_service.files().insert(body=body, media_body=media_body).execute()
end = time.clock()
print ("uploading 25K: "+ str(end-start))

downloadUrl = file.get('downloadUrl')
# downloading csv file with 25000 records and measuring time
start = time.clock()
resp, content = drive_service._http.request(downloadUrl)
if resp.status == 200:
    outstream = open('25K-download.csv', 'wb')
    outstream.write(content)
    outstream.close()
    end = time.clock()
    print ("downloading 25K: "+ str(end-start))

# Path to the file to upload - uploading csv file with 100000 records and measuring time
FILENAME = 'D:\\books\\semester4\\Cloud_Computing\\Project5-GWE\\100K.csv'

# Insert a file
media_body = MediaFileUpload(FILENAME, mimetype='text/csv', resumable=True)
body = {
  'title': '100K',
  'description': 'A test document',
  'mimeType': 'text/csv'
}
start = time.clock()
file = drive_service.files().insert(body=body, media_body=media_body).execute()
end = time.clock()
print ("uploading 100K: "+ str(end-start))

downloadUrl = file.get('downloadUrl')
# downloading csv file with 100000 records and measuring time
start = time.clock()
resp, content = drive_service._http.request(downloadUrl)
if resp.status == 200:
    outstream = open('100K-download.csv', 'wb')
    outstream.write(content)
    outstream.close()
    end = time.clock()
    print ("downloading 100K: "+ str(end-start))
