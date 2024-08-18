from googleapiclient.discovery import build
import pandasDF as PD
import MongoDB as MDB
import SQLDB as SDB

#To get the API Key : https://console.cloud.google.com/ ---> AIzaSyB4fA7kotAiXThSgRxPcj7EmTBZrT79hTc

api_service_name = "youtube"
api_version = "v3"
api_key = "YOUR_API_KEY"

youtube = build(api_service_name, api_version, developerKey=api_key)

while(True):
    channel_name = input('Enter the name of the youtube channel: ')
    search = youtube.search().list(
        part='snippet',
        q=channel_name,
        maxResults=1
    )
    search_response = search.execute()
    channel_name = search_response['items'][0]['snippet']['channelTitle']
    print("You have searched "+channel_name)
    print("Press 'y' to Confirm.. Press 'n' to re-enter the channel name")
    answer = input()
    if(answer=='y' or answer=='Y'):
        break
    else:
        continue

channel_id = search_response['items'][0]['snippet']['channelId']

MDB.clear_Mongodb()

request = youtube.channels().list(
    part="snippet,contentDetails,statistics",
    id=channel_id
)
response = request.execute()

MDB.channel_insert(response['items'][0])



print('Select from the below options:')

print('''

1. Most Recent Video
      
2. Most Liked Video of last 10 Videos
      
3. Most Viewed Video of last 10 Videos
      
4. Most Commented Video of last 10 Videos
      
5. Sum of Likes, Comments, Views of the last 10 Videos
      
6. Total number of videos posted in the channel, their subscriber's count and their viewcount
''')
while(True):
    option = input("Enter the Option: ")
    if(option in ['1','2','3','4','5','6']):
        break
    else:
        print("Please Enter a value from 1 to 6")
        continue

list_of_video_ids = []

activity_request = youtube.activities().list(
    part="snippet,contentDetails",
    channelId=channel_id,
    maxResults=50
)
activity = activity_request.execute()
for i in activity['items']:
    if(i['snippet']['type']=='upload'):
        list_of_video_ids.append(i['contentDetails']['upload']['videoId'])
        if(len(list_of_video_ids))==10:
            break

for i in list_of_video_ids:
    video_request = youtube.videos().list(
    part="snippet,id,status,contentDetails,statistics,player",
    id=str(i)
    )
    video = video_request.execute()
    MDB.insert_video(video['items'][0])


video_data = MDB.find_video()

video_data = PD.video_data(video_data)

channel_data = MDB.find_channel()

channel_data = PD.channel_data(channel_data)

SDB.insert_channel(channel_data)
SDB.insert_videos(video_data)

if(option=='1'):
    print("video: " + video_data['thumbnailurl'][0])
    print("link: " + video_data['embedHtml'][0])
elif(option=='2'):
    ind = video_data[['likeCount']].idxmax()
    print("video: " + video_data['thumbnailurl'][ind['likeCount']])
    print("link: " + str(video_data['embedHtml'][ind['likeCount']]))
elif(option=='3'):
    ind = video_data[['viewCount']].idxmax()
    print("video: " + video_data['thumbnailurl'][ind['viewCount']])
    print("link: " + video_data['embedHtml'][ind['viewCount']])
elif(option=='4'):
    ind = video_data[['commentCount']].idxmax()
    print("video: " + video_data['thumbnailurl'][ind['commentCount']])
    print("link: " + video_data['embedHtml'][ind['commentCount']])
elif(option=='5'):
    max_like = video_data[['likeCount']].sum()
    max_comment = video_data[['commentCount']].sum()
    max_view = video_data[['viewCount']].sum()
    print("Sum of Likes: "+str(max_like['likeCount']))
    print("Sum of Commets: "+str(max_comment['commentCount']))
    print("Sum of Views: "+str(max_view['viewCount']))
else:
    print("Total Videos: "+ str(channel_data['videoCount'][0]))
    print("Total Subscribers: "+ str(channel_data['subscriberCount'][0]))
    print("Total View Count: "+ str(channel_data['viewCount'][0]))