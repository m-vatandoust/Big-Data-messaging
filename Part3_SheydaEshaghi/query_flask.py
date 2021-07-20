from flask import Flask
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import WhiteListRoundRobinPolicy, DowngradingConsistencyRetryPolicy, ConsistencyLevel
from cassandra.query import tuple_factory
import datetime
from persiantools.jdatetime import JalaliDate
import json
from flask import request
app = Flask(__name__)


@app.route('/test')
def test():
    return 'It is a Test!'


@app.route('/LastOneHour')
def LastOneHour():
    profile = ExecutionProfile(
        load_balancing_policy=WhiteListRoundRobinPolicy(['127.0.0.1']),
        retry_policy=DowngradingConsistencyRetryPolicy(),
        consistency_level=ConsistencyLevel.LOCAL_QUORUM,
        serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
        request_timeout=200,
        row_factory=tuple_factory
    )
    cluster = Cluster(execution_profiles={
                      EXEC_PROFILE_DEFAULT: profile}, port=9042)
    session = cluster.connect('mykeyspace')

    query = "select * from total_posts"
    o = session.execute(query)
    out = []
    date = str(JalaliDate.today()).replace("-", "/")
    current_time = datetime.datetime.now().time()

    for i in o:
        time = i[0]
        time = time.split()
        if time[0] == date:
            tweet_time = [int(num) for num in time[1].split(":")]
            h, m = tweet_time[0], tweet_time[1]
            tweet_time_time = datetime.time(h, m, 00)
            cur_datetime = datetime.datetime.combine(
                datetime.date.today(), current_time)
            tw_datetime = datetime.datetime.combine(
                datetime.date.today(), tweet_time_time)
            diff = cur_datetime - tw_datetime
            diff_in_hours = diff.total_seconds()/3600
            if diff_in_hours <= 1:
                out.append(str(i))

    return json.dumps({"result": out})


@app.route('/Last24HourUser/')
def Last24HourUser():
    username = request.args.get("sender_name")
    # username = "naisi"
    profile = ExecutionProfile(
        load_balancing_policy=WhiteListRoundRobinPolicy(['127.0.0.1']),
        retry_policy=DowngradingConsistencyRetryPolicy(),
        consistency_level=ConsistencyLevel.LOCAL_QUORUM,
        serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
        request_timeout=200,
        row_factory=tuple_factory
    )
    cluster = Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: profile})
    session = cluster.connect('mykeyspace')

    query = f"select * from userNames where sender_name='{username}' ALLOW FILTERING"
    o = session.execute(query)
    out = []
    # date = str(JalaliDate.today()).replace("-", "/")
    current_time = datetime.datetime.now().time()
    for i in o:
        print(i)
        time = i[0]
        time = time.split()
        date = JalaliDate(*[int(num)
                          for num in time[0].split("/")]).to_gregorian()
        print(date)
        tweet_time = [int(num) for num in time[1].split(":")]
        h, m = tweet_time[0], tweet_time[1]
        tweet_time_time = datetime.time(h, m, 00)
        cur_datetime = datetime.datetime.combine(
            datetime.date.today(), current_time)
        tw_datetime = datetime.datetime.combine(
            date, tweet_time_time)
        diff = cur_datetime - tw_datetime
        diff_in_hours = diff.total_seconds()/3600
        if diff_in_hours <= 24:
            out.append(str(i))
    return json.dumps({"result": out})


@app.route('/Hashtag/')
def Hashtag():
    hashtag = request.args.get("hashtag")
    start = request.args.get("start")
    end = request.args.get("end")
    profile = ExecutionProfile(
        load_balancing_policy=WhiteListRoundRobinPolicy(['127.0.0.1']),
        retry_policy=DowngradingConsistencyRetryPolicy(),
        consistency_level=ConsistencyLevel.LOCAL_QUORUM,
        serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
        request_timeout=200,
        row_factory=tuple_factory
    )
    cluster = Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: profile})
    session = cluster.connect('mykeyspace')
    # date = request.args.get("date")
    print("input hashtag", hashtag)
    query = "SELECT * FROM hashtags where hashtag='#{hashtag}' ALLOW FILTERING"
    #query = "select * from hashtags"
    o = session.execute(query)
    out = []
    for i in o:
        # print(i)
        print(i[1].encode())
        time = i[0]
        time = time.split()
        tweet_date = JalaliDate(*[int(num)
                                  for num in time[0].split("/")]).to_gregorian()

        start_date = JalaliDate(*[int(num)
                                for num in start.split("/")]).to_gregorian()
        end_date = JalaliDate(*[int(num)
                              for num in end.split("/")]).to_gregorian()

        if (tweet_date - start_date).total_seconds() > 0 and (end_date - tweet_date).total_seconds() > 0:
            out.append(str(i))

    return json.dumps({"result": out})


if __name__ == '__main__':
    app.run(debug=True)
