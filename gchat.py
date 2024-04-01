import os, json
from datetime import datetime
import pytz

class GoogleChatDirectoryLoader:
    """
    This loader does not need any Google API credentials, since it is currently not possible to extract all Chat messages by API.
    Instead, download them from takeout.google.com, unpack the folder "Google Chat", then call the "load" method on the filepaths mentioned.

    """
    def __init__(self) -> None:
        pass

    def datetime_to_epoch(self, datetime_str):
        if datetime_str == 'Unknown Date':
            return None
        datetime_format = "%A, %B %d, %Y at %I:%M:%S %p %Z"
        try:
            datetime_obj = datetime.strptime(datetime_str.replace("UTC", "").strip(), datetime_format.replace("%Z", "").strip())
            datetime_obj_utc = datetime_obj.replace(tzinfo=pytz.utc)
            epoch_time = int(datetime_obj_utc.timestamp())
            return epoch_time
        except ValueError:
            return None

    def get_space_name(self, user_info_path, message_id):
        """
        helper function only used inside 'process_spaces'
        basically just matches space ID to space name
        """
        with open(user_info_path, 'r', encoding='utf-8') as file:
            user_info_data = json.load(file)
        group_id_part = message_id.split('/')[0]
        membership_info = user_info_data['membership_info']
        for item in membership_info:
            if group_id_part in item['group_id']:
                if 'group_name' in item and item['group_name'] is not None:
                    return item['group_name']
                else:
                    continue
        return 'Unknown Space'

    def process_spaces(self, file_path, user_info_path):
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        messages = data['messages']
        if messages:
            space_name = self.get_space_name(user_info_path, messages[0].get('message_id',''))
        else:
            space_name = 'Unknown Space'
        result = []
        for i in range(10, len(messages), 19):
            middlemessage = messages[i]
            startmessage = messages[i-10]
            metadata = {
                'source': 'chats',
                'type':'space',
                'name': space_name,
                'message_id': startmessage.get('message_id','Unknown ID'),
                'date': middlemessage.get('created_date','Unknown Date'),
                'unix_time': self.datetime_to_epoch(middlemessage.get('created_date','Unknown Date')),
                'url': f'https://chat.google.com/room/{startmessage.get("message_id","")}'
            }
            text_parts = []
            for j in range(max(0, i-10), min(len(messages), i+11)):
                j_message = messages[j]
                j_creator = j_message.get('creator',{})
                j_name = j_creator.get('name','Unknown')
                j_text = j_message.get('text','No text available')
                part = f"{j_name}\n{j_text}\n\n"
                text_parts.append(part)
            text_str = ''.join(text_parts)
            result.append({
                'metadata': metadata,
                'text': text_str
            })
        return result

    def process_dms(self, file_path):
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        messages = data['messages']
        unique_names = []
        for message in messages:
            creator_name = message.get('creator', {}).get('name', 'Unknown')
            if creator_name not in unique_names:
                unique_names.append(creator_name)
        result = []
        for i in range(10, len(messages), 19):
            middlemessage = messages[i]
            startmessage = messages[i-10]
            metadata = {
                'source':'chats',
                'type':'dm',
                'name':unique_names,
                'message_id':startmessage.get('message_id','Unknown ID'),
                'date':middlemessage.get('created_date','Unknown Date'),
                'unix_time':self.datetime_to_epoch(middlemessage.get('created_date','Unknown Date')),
                'url':f'https://chat.google.com/dm/{startmessage.get("message_id","")}'
            }
            text_parts = []
            for j in range(max(0, i-10), min(len(messages), i+11)):
                j_message = messages[j]
                j_creator = j_message.get('creator',{})
                j_name = j_creator.get('name','Unknown')
                j_text = j_message.get('text','No text available')
                part = f"{j_name}\n{j_text}\n\n"
                text_parts.append(part)
            text_str = ''.join(text_parts)
            result.append({
                'metadata': metadata,
                'text': text_str
            })
        return result

    def load(self, group_folder_path, user_info_path):
        """
        - Main function to process all G-Chat messages and line them up for indexing into a Vector DB.
        - Just pass the relative path to the "group" folder from your G-Chat zip download, as well as the path to the "user_info" JSON file.

        - Returns a single list of dictionaries, including all messages from all spaces, plus their metadata (space name, timestamp, id/URL).
        - In each space or DM channel it will walk a sliding window across the list of messages, storing 21 consecutive messages per document.
        - The individual documents overlap by 2 messages each. This in theory eliminates the need for addinitional chunking...
        - ...however, it is advisable to layer a standard, large window chunker on top, just to always stay in the embedding model's token limits.

        - For each document, the middle message of the block of 21 messages will provide the timestamp for the metadata. 
        - The ID and URL will be created from the 1st message of the block, so the link will always open the top of the message block.

        - Media files and message attachments are not processed, in-text URLs will just be processed as natural language.

        Example usage -> simply loop over the returned list:

        for item in list:
            rag_pipeline.run(text=item['text'], metadata=item['metadata'])

        """
        all_messages = []
        for root, dirs, files in os.walk(group_folder_path, user_info_path):
            for dir_name in dirs:
                subfolder_path = os.path.join(root, dir_name)
                messages_file_path = os.path.join(subfolder_path, 'messages.json')
                if os.path.exists(messages_file_path):
                    if "dm" in dir_name.lower():
                        processed_data = self.process_dms(messages_file_path)
                    elif "space" in dir_name.lower():
                        processed_data = self.process_spaces(messages_file_path, user_info_path)
                    else:
                        continue
                    all_messages.extend(processed_data)
        print(f"{len(all_messages)} threads successfully processed!")            
        return all_messages