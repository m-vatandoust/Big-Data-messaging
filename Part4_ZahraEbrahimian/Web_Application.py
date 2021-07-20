from flask import Flask, render_template, url_for, request, redirect
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
from redis_Management import RedisManagement, add_Redis

app = Flask(__name__)
redis_client = RedisManagement(exp_duration=3600*24*7)

class Lastresult():
    def __init__(self, client):
        self.client = client
    def get_queries(self):
        self.default_nuser = str(sum([int(x) for x in redis_client.get_times(10, 'totalPosts', 'hour')]))
        self.default_nposts = str(sum([int(x) for x in redis_client.get_times(1, 'totalPosts', 'day')]))
        self.default_nsymbols = str(sum([int(x) for x in redis_client.get_times(10, 'totalHashtags', 'hour')]))

last_result = Lastresult(redis_client)
@app.route('/', methods=['POST', 'GET'])
def index():
    last_result.get_queries()
    return render_template('index.html', result=last_result)

@app.route('/hashtags/', methods=['GET'])
def hashtags():
    results = redis_client.get_list('hashtags', 0, -1)
    return render_template('hashtags.html', results=results)

@app.route('/posts/', methods=['GET'])
def posts():
    results = redis_client.get_list('posts', 0, -1)
    return render_template('posts.html', results=results)

if __name__ == "__main__":
    app.run(debug=True)

	
	