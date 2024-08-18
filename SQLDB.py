import psycopg2 as ps
import sqlalchemy as sa
import datetime

# To create a conection in AWS DB : (https://ap-south-1.console.aws.amazon.com/rds/home?region=ap-south-1#databases:) -> Create DB and Enter the below Details.
#host="database-2.c7a8644iqhxt.ap-south-1.rds.amazonaws.com",database = 'youtube',user="postgres",password="Nishanth99",port="5432"
connection_Postgres = ps.connect(host="HOST",database = 'DB_NAME',user="USER",password="PASSWD",port="PORT")
cursor = connection_Postgres.cursor()
#postgresql://postgres:Nishanth99@database-2.c7a8644iqhxt.ap-south-1.rds.amazonaws.com:5432/youtube
sql_engine = sa.create_engine('YOUR_SQL_ENGINE')
connection = sql_engine.connect()


def insert_channel(data):
    timezone = "SET timezone = 'Asia/Kolkata';"
    cursor.execute(timezone)

    create_query = '''create table if not exists channels
    (
	id text not null,
	title varchar(50) not null,
	customUrl varchar(50),
	videoCount int,
	viewCount int,
	subscriberCount int,
	country varchar(20),
	thumbnail_URL text,
	thumbnail_resolution varchar(10),
	description text,
	publishedAt Timestamptz,
	inserted_time Timestamp default current_timestamp
    )'''
    cursor.execute(create_query)
    cursor.execute("commit")


    # cursor.execute("truncate table channels")
    # cursor.execute("commit")


    query = "insert into channels values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    for i in data.loc[data.index].values:
        details=[]
        for j in range(len(i)):
            details.append(i[j])
        cursor.execute(query,details)
    cursor.execute("commit")

def insert_videos(data):
    data['insert_time'] = datetime.datetime.now()
    data.to_sql('videos',sql_engine,if_exists='append',index=False)