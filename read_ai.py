import requests, json, copy
from datetime import datetime, timedelta


class ACsReadAILoader:
    """
    This class connects to the meeting notetaker app "Read.AI" 
    and downloads all textual meeting data/metadata 
    and prepares it for a RAG indexing pipeline.

    Only user's Read.AI email and PW needed, no API key.

    """
    def __init__(self, user, password):
        self.user = user
        self.password = password
        self.access_token = self.authenticate()

    #authenticating the user:

    def authenticate(self):
        url = "https://api.read.ai/login/read"
        payload = {
            "email":self.user,
            "password":self.password,
            "action":"email"
        }
        headers = {
            "Content-Type": "application/json"
        }
        response = requests.post(url, json=payload, headers=headers)
        if response.status_code == 200:
            cookies = response.cookies.get_dict(domain=".read.ai")
            return cookies.get('access_token')
        else:
            print(f"Failed to authenticate, status code: {response.status_code}")
            return None

    #retrieving list of meeting-IDs, returning as list:

    def list_session_ids(self):
        url_list_session_ids = "https://api.read.ai/sessions"
        headers = {
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.9,de-DE;q=0.8,de;q=0.7",
            "authorization": f"Bearer {self.access_token}",
            "origin": "https://app.read.ai",
            "referer": "https://app.read.ai/",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        }
        params = {
            "start_date": datetime.utcfromtimestamp(0).isoformat(),
            "end_date": (datetime.utcnow() + timedelta(days=1)).isoformat()
        }
        response = requests.get(url=url_list_session_ids, headers=headers, params=params)
        if response.status_code == 200:
            response_data = response.json()
            sessions = []
            for session in response_data:
                id = session.get('id')
                sessions.append(id)
            return(sessions)
        else:
            return(f"Failed to fetch data: {response.status_code} - {response.text}")

    #different formatting helper methods:

    def format_time_delta(self, milliseconds):
        hours, remainder = divmod(milliseconds/1000, 3600)
        minutes, seconds = divmod(remainder, 60)
        if hours > 0:
            return f"{int(hours)}:{int(minutes):02d}:{int(seconds):02d}"
        else:
            return f"{int(minutes)}:{int(seconds):02d}"
        
    def format_date(self, start_time):
        date_object = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S.%f")
        formatted_date = date_object.strftime("%d. %B %Y")
        return formatted_date
    
    def timestamp_to_unix(self, start_time):
        date_object = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S.%f")
        unix_code = int(date_object.timestamp())
        return unix_code

    #retrieving all available data from different endpoints:

    def get_transcript_data(self, session_id):
        url_session = f"https://api.read.ai/sessions/{session_id}/transcript"
        headers = {
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.9,de-DE;q=0.8,de;q=0.7",
            "authorization": f"Bearer {self.access_token}",
            "origin": "https://app.read.ai",
            "referer": "https://app.read.ai/",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        }
        response = requests.get(url=url_session, headers=headers)
        if response.status_code == 200:
            return(response)
        else:
            return(f"Failed to fetch data: {response.status_code} - {response.text}")
        
    def get_session_data(self, session_id):
        url_session = f"https://api.read.ai/sessions/{session_id}"
        headers = {
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.9,de-DE;q=0.8,de;q=0.7",
            "authorization": f"Bearer {self.access_token}",
            "origin": "https://app.read.ai",
            "referer": "https://app.read.ai/",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        }
        response = requests.get(url=url_session, headers=headers)
        if response.status_code == 200:
            return(response)
        else:
            return(f"Failed to fetch data: {response.status_code} - {response.text}")
    
    def get_postcall_data(self, session_id):
        url_session = f"https://api.read.ai/sessions/{session_id}/metrics/post-call"
        headers = {
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.9,de-DE;q=0.8,de;q=0.7",
            "authorization": f"Bearer {self.access_token}",
            "origin": "https://app.read.ai",
            "referer": "https://app.read.ai/",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        }
        response = requests.get(url=url_session, headers=headers)
        if response.status_code == 200:
            return(response)
        else:
            return(f"Failed to fetch data: {response.status_code} - {response.text}")
    
    #compiling the different datasets:

    def compile_transcript(self, transcript_response):
        data_json = transcript_response.json()
        compiled_transcript = ""
        for turn in data_json['data']['sessionTranscript']['turns']:
            turn_start_time = turn['words'][0]['startTime']-data_json['data']['sessionTranscript']['turns'][0]['words'][0]['startTime']
            speaker_name = turn['speaker']['name']
            compiled_transcript += f"{self.format_time_delta(turn_start_time)} - {speaker_name}:\n"
            turn_text = ' '.join(word['value'] for word in turn['words'])
            compiled_transcript += turn_text + "\n\n" 
        return compiled_transcript
    
    def compile_action_items(self, transcript_response):
        data_json = transcript_response.json()
        action_items = []
        for item in data_json['data']['sessionTranscript']['actionItems']:
            action_items.append(item['text'])
        return action_items
    
    def compile_key_questions(self, transcript_response):
        data_json = transcript_response.json()
        key_questions = []
        for question in data_json['data']['sessionTranscript']['keyQuestions']:
            key_questions.append(question['text'])
        return key_questions
    
    def compile_summary(self, transcript_response):
        data_json = transcript_response.json()
        try:
            summary = data_json['data']['sessionTranscript']['summary']['text']
            return summary
        except:
            return ""
    
    def compile_metadata(self, session_response):
        metadata_json = session_response.json()
        metadata = {
            'source':'meetings',
            'id':metadata_json['id'],
            'title':metadata_json['title'],
            'date':self.format_date(metadata_json['start_time']),
            'unix_time':self.timestamp_to_unix(metadata_json['start_time']),
            'meeting_platform':metadata_json['meeting_platform'],
            'meeting_id':metadata_json['meeting_id'],
            'start_time':metadata_json['start_time'],
            'end_time':metadata_json['end_time'],
            'url':f"https://app.read.ai/analytics/meetings/{metadata_json['id']}"
            } 
        return metadata

    def compile_participants(self, postcall_response):
        data_json = postcall_response.json()
        participants = data_json['participants']
        return participants

    #class's main method:

    def lazyload(self, session_id):
        """
        Main loading method:

        Takes in Read AI session ID and returns a list of 4 arrays. 
        Each of the 4 arrays is optimized to use as a langchain or llamindex 'Document':
        
        1. {Metadata:Meeting Metadata, Text:Meeting Summary}
        2. {Metadata:Meeting Metadata, Text:Key questions}
        3. {Metadata:Meeting Metadata, Text:Action items}
        4. {Metadata:Meeting Metadata, Text:Meeting Transcript}

        Example usage:

        1. run 'list_session_ids', store resulting list in memory
        2. loop through result list, running 'lazyload' on each session ID
        3. nested loop through result of 'lazyload':
        4. doc = [Document(text=result.get('text'), metadata=result.get('metadata'))]
        5. pipeline.run(documents=doc)
        """
        documents = []
        session_response = self.get_session_data(session_id)
        transcript_response = self.get_transcript_data(session_id)
        postcall_response = self.get_postcall_data(session_id)
        speakers = self.compile_participants(postcall_response)
        metadata = self.compile_metadata(session_response)
        metadata['speakers'] = speakers  
        summary = self.compile_summary(transcript_response)
        key_questions = self.compile_key_questions(transcript_response)
        action_items = self.compile_action_items(transcript_response)
        transcript = self.compile_transcript(transcript_response)
        summary_metadata = copy.deepcopy(metadata)
        summary_metadata['text_type'] = 'summary'
        key_questions_metadata = copy.deepcopy(metadata)
        key_questions_metadata['text_type'] = 'key_questions'
        action_items_metadata = copy.deepcopy(metadata)
        action_items_metadata['text_type'] = 'action_items'
        transcript_metadata = copy.deepcopy(metadata)
        transcript_metadata['text_type'] = 'transcript'
        documents.append({'metadata':summary_metadata, 'text':str(summary)})
        documents.append({'metadata':key_questions_metadata, 'text':str(key_questions)})
        documents.append({'metadata':action_items_metadata, 'text':str(action_items)})
        documents.append({'metadata':transcript_metadata, 'text':str(transcript)})
        return documents