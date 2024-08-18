import pandas as pd
import re
import isodate

def get_video_link(embedHtml):
    # Regular expression to find the src attribute value
    match = re.search(r'src="([^"]+)"', embedHtml)
    if match:
        src = match.group(1)
        # If the src starts with "//", prepend "https:"
        if src.startswith("//"):
            src = "https:" + src

    return(src)

def convert_duration(duration):
    duration_str = str(duration)
    # Parse the duration
    duration = isodate.parse_duration(duration_str)
    return duration


def video_data(video_data):
    df_video_data = pd.DataFrame(video_data)
    df_video_data

    df_snippet_data = pd.json_normalize(df_video_data['snippet'])

    df_snippet_data.drop(columns=['thumbnails.default.url','thumbnails.default.width','thumbnails.default.height',
                              'thumbnails.medium.url','thumbnails.medium.width','thumbnails.medium.height',
                              'thumbnails.high.url','thumbnails.high.width','thumbnails.high.height',
                              'thumbnails.standard.url','thumbnails.standard.width','thumbnails.standard.height'
                              ],inplace=True)
    df_video_data = pd.concat([df_video_data,df_snippet_data],axis=1)

    df_content_data = pd.json_normalize(df_video_data['contentDetails'])
    df_video_data = pd.concat([df_video_data,df_content_data],axis=1)

    df_stats_data = pd.json_normalize(df_video_data['statistics'])
    df_video_data = pd.concat([df_video_data,df_stats_data],axis=1)

    df_player_data = pd.json_normalize(df_video_data['player'])

    video_link = []
    for i in df_player_data['embedHtml']:
        video_link.append(str(get_video_link(i)))
    df_player_data['embedHtml'] = video_link

    df_video_data = pd.concat([df_video_data,df_player_data],axis=1)

    df_video_data.drop(columns=['snippet','contentDetails','statistics','player'],inplace=True)

    df_video_data['thumbnailresolution'] = str(df_video_data['thumbnails.maxres.width'][0]) + 'x' + str(df_video_data['thumbnails.maxres.height'][0])
    df_video_data.drop(columns=['thumbnails.maxres.width','thumbnails.maxres.height'], inplace = True)

    df_video_data = df_video_data.rename(columns={'thumbnails.maxres.url': 'thumbnailurl'})
    df_video_data['runtime'] = df_video_data['duration'].apply(convert_duration)

    time_str = []
    for i in df_video_data['runtime']:
        time_str.append(str(i)[-8:])
        
    df_video_data["runtime"] = time_str

    new_order_video = ['id', 'channelId', 'channelTitle', 'title', 'defaultLanguage', 'defaultAudioLanguage', 'categoryId', 'duration', 'runtime', 'caption',
             'licensedContent', 'viewCount', 'likeCount', 'commentCount', 'thumbnailresolution', 'thumbnailurl', 'publishedAt', 'embedHtml']

    df_video_data = df_video_data[new_order_video]
    df_video_data['categoryId'] = df_video_data['categoryId'].astype(int)
    df_video_data['caption'] = df_video_data['caption'].astype(bool)
    df_video_data['viewCount'] = df_video_data['viewCount'].astype(int)
    df_video_data['likeCount'] = df_video_data['likeCount'].astype(int)
    df_video_data['commentCount'] = df_video_data['commentCount'].astype(int)
    df_time = df_video_data['publishedAt']
    for i in range(10):
        df_video_data['publishedAt'] = pd.to_datetime(df_video_data['publishedAt'])
    df_video_data['publishedAt']

    return df_video_data


def channel_data(channel_data):
    df_channel_data = pd.DataFrame(channel_data)

    df_snippet_detail = pd.json_normalize(df_channel_data['snippet'])
    df_channel_data = pd.concat([df_channel_data,df_snippet_detail],axis=1)

    df_stats_detail = pd.json_normalize(df_channel_data['statistics'])
    df_channel_data = pd.concat([df_channel_data,df_stats_detail],axis=1)

    df_channel_data.drop(columns = ['snippet','statistics'], inplace = True)

    df_channel_data = df_channel_data.rename(columns={'thumbnails.high.url': 'thumbnailurl'})

    df_channel_data['thumbnailresolution'] = str(df_channel_data['thumbnails.high.width'][0]) + 'x' + str(df_channel_data['thumbnails.high.height'][0])
    df_channel_data.drop(columns=['thumbnails.high.width','thumbnails.high.height'], inplace = True)

    new_order_channel = ['id', 'title', 'customUrl', 'videoCount', 'viewCount', 'subscriberCount', 'country', 'thumbnailurl', 'thumbnailresolution', 'description', 'publishedAt']
    df_channel_data = df_channel_data[new_order_channel]

    df_channel_data['videoCount'] = df_channel_data['videoCount'].astype(int)
    df_channel_data['viewCount'] = df_channel_data['viewCount'].astype(int)
    df_channel_data['subscriberCount'] = df_channel_data['subscriberCount'].astype(int)
    df_channel_data['publishedAt'] = pd.to_datetime(df_channel_data['publishedAt'])
    timestamp = pd.Timestamp(df_channel_data['publishedAt'][0])
    timestamp_str = timestamp.strftime('%Y-%m-%d')
    df_channel_data['publishedAt'] = timestamp_str

    return df_channel_data