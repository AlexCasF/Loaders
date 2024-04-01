import os.path, re, base64, json, csv
from tqdm import tqdm
from email import message_from_bytes
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

class GoogleMailLoader:
    """
    GoogleMailLoader facilitates the authentication with the Gmail API 
    and the retrieval of email messages based on specified queries.

    This class abstracts the process of connecting to the Gmail API, executing search queries, 
    and processing email messages to extract email content and metadata.

    Attributes:
        credentials_path (str): File path to the OAuth 2.0 credentials for accessing the Gmail API.

    Main methods:
        load(query): Retrieves emails matching the given query.
        email_metadata_to_csv(): Retrieves list of all emails' metadata and saves to CSV file.
    """
        
    def __init__(self, credentials_path):
        """
        Initializes the ACsGmailLoader with the path to the credentials file.

        Args:
            credentials_path (str): path to JSON file containing OAuth 2.0 creds for Gmail API.

        How to get a credentials file:

        1. create a Google Cloud Project
        2. enable Gmail API in "APIs & Services"
        3. in the API settings, go to "Credentials"
        4. "Create OAuth client ID" > "Web application"
        5. set "Authorized redirect URIs" to your app's localhost:port or web URL
        6. create new client secret and download as JSON file
        7. pass file path to kwarg of loader instance, as shown below

        ---

        from gmail import GoogleMailLoader

        loader = GoogleMailLoader(credentials_path='credentials.json')
        """
        self.credentials_path = credentials_path
        self.scopes = ['https://www.googleapis.com/auth/gmail.readonly']
        self.service = self.get_gmail_service()

    def get_gmail_service(self):
        """
        Private method to authenticate with the Gmail API using the provided OAuth 2.0 credentials.

        Returns:
            googleapiclient.discovery.Resource: An authenticated Gmail API service instance.
        """
        creds = None
        if os.path.exists('token.json'):
            creds = Credentials.from_authorized_user_file('token.json', self.scopes)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(self.credentials_path, self.scopes)
                creds = flow.run_local_server(port=50590, prompt='consent')
            with open('token.json', 'w') as token_file:
                token_file.write(creds.to_json())
        return build('gmail', 'v1', credentials=creds)

    def get_messages(self, query):
        """"
        Retrieves and returns a list of message IDs from the Gmail API.
        """
        messages = []
        page_token = None
        while True:
            response = self.service.users().messages().list(userId='me', q=query, pageToken=page_token).execute()
            messages.extend(response.get('messages', []))
            page_token = response.get('nextPageToken')
            if not page_token:
                break
        if not messages:
            print('No messages found.')
        else:
            print(f'Total message IDs retrieved: {len(messages)}')
        return messages

    def clean_text(self, input_text):
        """
        Helper function to cleanup email text bodies from HTML artefacts.
        Optimizes results for followup processes, such as creating vector embeddings.
        """
        pattern_links_and_brackets = re.compile(r'\[([^]]+)\]\([^)]+\)|\[.*?\]')
        cleaned_text = re.sub(pattern_links_and_brackets, r'\1 ', input_text)
        pattern_angle_brackets = re.compile(r'<.*?>|>|<')
        cleaned_text = re.sub(pattern_angle_brackets, ' ', cleaned_text)
        replacements = {
            "\r\n\t": " ",
            "\r\n": " ",
            "\n": " ",
            "\\": " ",
            "\\'": "'",
            "?": "? ",
            "!": "! ",
        }
        for old, new in replacements.items():
            cleaned_text = cleaned_text.replace(old, new)
        cleaned_text = re.sub(r' +', ' ', cleaned_text)
        cleaned_text = re.sub(r'([^\w\s])\1*', r'\1', cleaned_text)
        return cleaned_text

    def get_message_details(self, message_id):
        """
        Takes in message ID and returns email as dictionary object in the style of a langchain Document.
        """
        msg = self.service.users().messages().get(userId='me', id=message_id, format='raw').execute()
        msg_raw = base64.urlsafe_b64decode(msg['raw'].encode('ASCII'))
        mime_msg = message_from_bytes(msg_raw)
        body = ''
        if mime_msg.is_multipart():
            for part in mime_msg.walk():
                if part.get_content_type() == 'text/plain':
                    body = part.get_payload(decode=True).decode()
                    break
        else:
            if mime_msg.get_content_type() == 'text/plain':
                body = mime_msg.get_payload(decode=True).decode()
        text = self.clean_text(body)
        details = {
            'text': text,
            'metadata': {
                'source':'emails',
                'subject': mime_msg['Subject'],
                'date': mime_msg['Date'],
                'from': mime_msg['From'],
                'to': mime_msg['To'],
                'cc': mime_msg.get('Cc', ''),
                'bcc': mime_msg.get('Bcc', ''),
                'unix_time': int(msg.get('internalDate'))//1000,
                'message_id': message_id,
                'url': f'https://mail.google.com/mail/u/0/?ogbl#inbox/{message_id}',
                'content_type': mime_msg.get_content_type()
            }
        }
        return details

    def load(self, query):
        """
        Main method of this class - loads emails based on a given query and processes them 
        to extract their content and metadata.

        The return is optimized to be used in a langchain or llama-index pipeline 
        or similar use cases.

        This method searches for emails matching the specified query, 
        retrieves them from the Gmail account associated with the authenticated user, 
        and processes each email to remove hyperlink URLs and extract metadata.

        Args:
            query (str): The Gmail query string for retrieving emails. 
            This can include various search operators as documented here: 
            https://support.google.com/mail/answer/7190

        Returns:
            list of dict: A list of dictionaries, where each dictionary contains the
            'text' (email body with hyperlinks removed) and 
            'metadata' (email metadata such as subject, date, sender, etc.) of each email.

        Process can take up to 30 min per 1000 emails! (depending on connection speed)
        """
        messages = self.get_messages(query)
        emails_details = []
        failed_downloads = []
        print('Starting download...')
        for msg in tqdm(messages, desc='Downloading emails', unit='email'):
            try:
                details = self.get_message_details(msg['id'])
                emails_details.append(details)
            except Exception as e:
                failed_content = self.service.users().messages().get(userId='me', id=msg['id']).execute()
                failed_downloads.append({
                    'message_id': msg['id'],
                    'error_message': str(e),
                    'email': str(failed_content)
                })
        if failed_downloads:
            print(f'Download complete with some errors. Failed downloads: {len(failed_downloads)}. See JSON file for details.')
            with open('failed_downloads.json', 'w', encoding='utf-8') as f:
                json.dump(failed_downloads, f, ensure_ascii=False, indent=4)
        else:
            print('Download complete without errors!') 
        return emails_details
    
    def email_metadata_to_csv(self, filter=""):
        """
        This method pulls all your emails' metadata from your Gmail account 
        and saves it to a CSV file in the root directory, 
        which then can be used for statistical purposes or 
        for finding out which query to use for the 'load' method.

        You can pass a timeframe filter string as a kwarg to the method such as:
        filter='after:2023/12/31'
        filter='before:2024/03/16'
        filter='after:2022/12/31 before:2024/01/01'

        You can also add other filters (check https://support.google.com/mail/answer/7190),
        however this would kind of defeat the purpose - you can always filter the CSV later.

        If empty, ALL emails will be processed
        
        Usage example:

        First: get Gmail credentials JSON file, as explained above.

        ---

        from gmail import GoogleMailLoader

        loader = GoogleMailLoader(credentials_path='credentials.json')
        loader.mail_metadata_to_csv()

        ---

        Process can take up to 10 min per 1000 emails! (depending on connection speed)
        """
        msg_ids = self.get_messages(f'in:anywhere {filter}')
        csv_file_name = 'email_metadata.csv'
        headers = ['Subject', 'Date', 'From', 'To', 'Cc', 'Bcc', 'Message-ID', 'Content-Type']
        with open(csv_file_name, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=headers)
            writer.writeheader()
            for id in tqdm(msg_ids, desc='Processing emails', unit='email'):
                try:
                    msg = self.service.users().messages().get(userId='me', id=id['id'], format='metadata').execute()
                    metadata = {}
                    headers = msg.get('payload', {}).get('headers', [])
                    header_names = ['Subject', 'Date', 'From', 'To', 'Cc', 'Bcc', 'Message-ID', 'Content-Type']
                    for header in headers:
                        if header['name'] in header_names:
                            metadata[header['name']] = header.get('value', '')
                    for name in header_names:
                        metadata.setdefault(name, '')
                    writer.writerow(metadata)
                except Exception as e:
                    print(f"Error processing message ID {id['id']}: {e}")
            print(f'Metadata for {len(msg_ids)} emails has been saved to {csv_file_name}.')