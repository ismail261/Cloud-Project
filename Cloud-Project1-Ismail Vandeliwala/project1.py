# Include the Dropbox SDK
import dropbox
import gnupg
import os
import sys
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class MyHandler(FileSystemEventHandler):
    def on_modified(self, event):
        path = event.src_path
        dir_entry = path[path.rindex("\\") + 1:len(path)]
        # path for reading files from local folder to be uploaded
        f = open(inputFolder+'/'+dir_entry, 'rb')
        sign_data = gpg.sign_file(f)
        encrypted_data = gpg.encrypt(sign_data.data,key.fingerprint)
        response = client.put_file('/input/'+dir_entry, encrypted_data.data, overwrite=True)
        print ("uploaded:", response)

        f, metadata = client.get_file_and_metadata('/input/'+dir_entry)
        # path for files in local folder to be downloaded
        filename = outputFolder+'/'+dir_entry
        out = open(filename, 'wb')
        out.write(f.read())

        out = open(filename, 'rb')
        decrypted_data = gpg.decrypt_file(out,output=filename)

        out = open(filename, 'rb')
        verified = gpg.verify_file(out)
        if verified:
            print ("File has been verified")
            print (verified)
        else: 
            print ("File not verified")
        
        out.close()
        
if __name__ == "__main__":
    # Get your app key and secret from the Dropbox developer website (change the app_key and app_secret)
    app_key = '50e2cqm9hcxr79y'
    app_secret = '7hfqt9sah5c0mgd'

    # path for reading files from local folder to be uploaded (do not change the format) (change the path according to your local directories)
    inputFolder = 'D:/books/semester4/Cloud_Computing/Project1-Dropbox/input'

    # path for files in local folder to be downloaded (do not change the format) (change the path according to your local directories)
    outputFolder = 'D:/books/semester4/Cloud_Computing/Project1-Dropbox/output'

    # path for files to read first time from input folder (do not change the format) (change the path according to your local directories)
    path = r'D:\\books\semester4\Cloud_Computing\Project1-Dropbox\input'

    # path to set the gpg home directory where all keys will be stored (do not change the format) (change the path according to your local directories)
    gpg_home = "D:\\books\semester4\Cloud_Computing\Project1-Dropbox\secret"
    gpg = gnupg.GPG(gnupghome=gpg_home)
    gpg.encoding = 'utf-8'

    input_data = gpg.gen_key_input(key_type="RSA", key_length=1024, name_real="Ismail-Cloud", name_email = "ismail.vandeliwala@gmail.com")
    key = gpg.gen_key(input_data)

    public_keys = gpg.list_keys()
    private_keys = gpg.list_keys(True)

    flow = dropbox.client.DropboxOAuth2FlowNoRedirect(app_key, app_secret)

    # Have the user sign in and authorize this token
    authorize_url = flow.start()
    print("1. Go to: " + authorize_url)
    print ("2. Click Allow (you might have to log in first)")
    print ("3. Copy the authorization code.")
    code = input("Enter the authorization code here: ")

    # This will fail if the user enters an invalid authorization code
    access_token, user_id = flow.finish(code)

    client = dropbox.client.DropboxClient(access_token)
    print ("linked account: ", client.account_info())

    for dir_entry in os.listdir(path):

        f = open(inputFolder+'/'+dir_entry, 'rb')
        sign_data = gpg.sign_file(f)
        encrypted_data = gpg.encrypt(sign_data.data,key.fingerprint)
        response = client.put_file('/input/'+dir_entry, encrypted_data.data, overwrite=True)
        print ("uploaded:", response)

        f, metadata = client.get_file_and_metadata('/input/'+dir_entry)
       
        filename = outputFolder+'/'+dir_entry
        out = open(filename, 'wb')
        out.write(f.read())

        out = open(filename, 'rb')
        decrypted_data = gpg.decrypt_file(out,output=filename)

        out = open(filename, 'rb')
        verified = gpg.verify_file(out)
        if verified:
            print ("File has been verified")
            print (verified)
        else: 
            print ("File not verified")
        
        out.close()
    
    event_handler = MyHandler()
    observer = Observer()
    observer.schedule(event_handler, inputFolder, recursive=False)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
