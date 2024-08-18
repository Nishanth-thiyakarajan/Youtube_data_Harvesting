import pymongo as pm

#To get the Connection code : https://cloud.mongodb.com/v2/66940921c5479449e13a2075#/overview
#mongodb+srv://Nishanth:Nishanth99@cluster0.1e2vg3x.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0
connection_MongoDB = pm.MongoClient("YOUR_CONNNECTION CODE")

channel_db = connection_MongoDB.Youtube.Channels
video_db = connection_MongoDB.Youtube.Videos


def clear_Mongodb():
    channel_db.drop()
    video_db.drop()

def channel_insert(data):
    channel_db.insert_one(data)

def insert_video(data):
    video_db.insert_one(data)


def find_video():
    data_list = []
    for i in video_db.find({},{"_id":0,'kind':0,'etag':0,'snippet.description':0,'snippet.type':0,'snippet.liveBroadcastContent':0,
                           'snippet.localized':0,'snippet.tags':0,'contentDetails.dimension':0,'contentDetails.definition':0,
                           'statistics.favoriteCount':0,'contentDetails.contentRating':0,'contentDetails.projection':0,'status':0}):
        data_list.append(i)

    return(data_list)

def find_channel():
    channel_list = []
    for i in channel_db.find({},{"_id":0,'kind':0,'etag':0,
                            'snippet.thumbnails.default':0,'snippet.thumbnails.medium':0,
                            'snippet.localized':0,'snippet.tags':0,
                            'contentDetails':0,'statistics.hiddenSubscriberCount':0}):
        channel_list.append(i)

    return channel_list