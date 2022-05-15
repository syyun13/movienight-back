import pymysql
import json

# Connection
connection = pymysql.connect(host=endpoint, user=username, passwd=password, db=database_name)

def lambda_handler(event, context):
    cursor = connection.cursor()
    if event['httpMethod'] == 'POST':
        body = json.loads(event['body'])
    
    if event['path'] == '/event':
        # ---------- GET ALL EVENT OF A USER
        if event['httpMethod'] == 'GET':
            params = event['queryStringParameters']
            user_id = params['userid']
            if user_id == 'all':
                query = 'SELECT events.id, event_name, event_date, movie_name, poster_img, count(*) as count FROM events join event_users on events.id = event_users.event_id group by event_users.event_id order by event_date'
            else:
                query = "SELECT events.id, event_name, event_date, movie_name, poster_img FROM events JOIN event_users ON events.id = event_users.event_id WHERE event_users.user_id = " + user_id + " order by event_date"
            cursor.execute(query)
            events = cursor.fetchall()
            if events is None:
                return {
                    'statusCode': 200,
                    'headers': {
                        "Access-Control-Allow-Origin" : "*"
                    },
                    'body': ''
                }
            else:
                return {
                    'statusCode': 200,
                    'headers': {
                        "Access-Control-Allow-Origin" : "*"
                    },
                    'body': json.dumps(events, default=str)
                }
        # ---------- CREATE NEW EVENT
        if event['httpMethod'] == 'POST':
            event_name = body['event_name']
            event_date = body['event_date']
            movie_name = body['movie_name']
            poster_img = body['poster_img']
            
            user_id = body['user_id']
            
            query = 'INSERT INTO events (event_name, event_date, movie_name, poster_img) values (%s, %s, %s, %s)'
            cursor.execute(query, (event_name, event_date, movie_name, poster_img))
            connection.commit()
            
            event_id = cursor.lastrowid
            query = 'INSERT INTO event_users (user_id, event_id) values (%s, %s)'
            cursor.execute(query, (user_id, event_id))
            connection.commit()
            
            g = body['guests']
            if g:
                guests = g.split(",")
                for guest in guests:
                    query = 'INSERT INTO event_users (user_id, event_id) values (%s, %s)'
                    cursor.execute(query, (guest, event_id))
                    connection.commit()

            return {
                'statusCode': 200,
                'headers': {
                  "Access-Control-Allow-Origin" : "*",  
                },
                'body': "New event created"
            }
        # ---------- DELETE EVENT
        if event['httpMethod'] == 'DELETE':
            params = event['queryStringParameters']
            event_id = params['eventid']

            query1 = 'DELETE FROM event_users WHERE event_id = %s'
            cursor.execute(query1, event_id)
            
            query2 = 'DELETE FROM events WHERE id = %s'
            cursor.execute(query2, event_id)
            connection.commit()

            return {
                'statusCode': 200,
                'headers': {
                  "Access-Control-Allow-Origin" : "*",  
                },
                'body': 'Deleted Event'
            }
    # ---------- REGISTER NEW USER
    if event['path'] == '/register' and event['httpMethod'] == 'POST':
        username = body['username']
        password = body['password']
        email = body['email']

        query = 'INSERT INTO users (username, password, email) values (%s, %s, %s)'
        cursor.execute(query, (username, password, email))
        connection.commit()
        return {
            'statusCode': 200,
            'headers': {
                "Access-Control-Allow-Origin": "*"
            },
            'body': cursor.lastrowid
        }
    # ---------- LOGIN
    if event['path'] == '/login' and event['httpMethod'] == 'POST':
        username = body['username']
        password = body['password']
        
        query = 'SELECT id, password FROM users WHERE username = %s'
        cursor.execute(query, username)
        correct = cursor.fetchone()
        
        if correct is None:
            return {
                'statusCode': 400,
                'headers': {
                    "Access-Control-Allow-Origin": "*"
                },
                'body': "User does not exist"
            }
        elif correct[1] == password:
            return {
                'statusCode': 200,
                'headers': {
                    "Access-Control-Allow-Origin" : "*"
                },
                'body': json.dumps(correct[0], default=str)
            }
        else:
            return {
                'statusCode': 400,
                'headers': {
                    "Access-Control-Allow-Origin" : "*"
                },
                'body': "Password is incorrect"
            }
    if event['path'] == '/user':
        # ---------- DELETE FOLLOWER/FOLLOWINGS
        if event['httpMethod'] == 'DELETE':
            params = event['queryStringParameters']
            follower = params['follower']
            user = params['user']
            query = 'DELETE FROM friends WHERE follower = %s and user = %s'
            cursor.execute(query, (follower, user))
            connection.commit()
            return {
                'statusCode': 200,
                'headers': {
                    "Access-Control-Allow-Origin" : "*"
                },
                'body': "Deleted following"
            }
            
        if event['httpMethod'] == 'GET':
            params = event['queryStringParameters']
            # ---------- GET USERS WHO ARE FOLLOWING THIS USER
            if 'following' in params:
                following = params['following']
                query = 'SELECT users.id, username FROM friends JOIN users ON friends.follower = users.id WHERE user = %s ORDER BY username'
                cursor.execute(query, following)
                users = cursor.fetchall()
                if users is None:
                    return {
                        'statusCode': 200,
                        'headers': {
                            "Access-Control-Allow-Origin" : "*"
                        },
                        'body': ""
                    }
                else:
                    return {
                        'statusCode': 200,
                        'headers': {
                            "Access-Control-Allow-Origin" : "*"
                        },
                        'body': json.dumps(users)
                    }
            # ---------- GET USERS THAT THIS USER IS FOLLOWING
            if 'follower' in params:
                follower = params['follower']
                query = 'SELECT users.id, username FROM friends JOIN users ON friends.user = users.id WHERE follower = %s ORDER BY username'
                cursor.execute(query, follower)
                users = cursor.fetchall()
                if users is None:
                    return {
                        'statusCode': 200,
                        'headers': {
                            "Access-Control-Allow-Origin" : "*"
                        },
                        'body': ""
                    }
                else:
                    return {
                        'statusCode': 200,
                        'headers': {
                            "Access-Control-Allow-Origin" : "*"
                        },
                        'body': json.dumps(users)
                    }
            # ---------- GET ALL USERS IN AN EVENT
            if 'eventid' in params:
                event_id = params['eventid']
                query = 'SELECT event_users.user_id, users.username, users.email FROM event_users JOIN users ON event_users.user_id = users.id WHERE event_id = %s'
                cursor.execute(query, event_id)
                guests = cursor.fetchall()
                
                if guests is None:
                    return {
                        'statusCode': 200,
                        'headers': {
                            "Access-Control-Allow-Origin" : "*"
                        },
                        'body': ""
                    }
                else:
                    return {
                        'statusCode': 200,
                        'headers': {
                            "Access-Control-Allow-Origin" : "*"
                        },
                        'body': json.dumps(guests)
                    }
            # ---------- CHECK IF USER EXISTS
            if 'username' in params:
                username = params['username']
                query = 'SELECT id FROM users WHERE username = %s'
                cursor.execute(query, username)
                user = cursor.fetchone()
                
                if user is None:
                    return {
                        'statusCode': 400,
                        'headers': {
                            "Access-Control-Allow-Origin": "*"
                        },
                        'body': "User does not exist"
                    }
                else:
                    return {
                        'statusCode': 200,
                        'headers': {
                            "Access-Control-Allow-Origin" : "*"
                        },
                        'body': json.dumps(user[0], default=str)
                    }
        # ---------- ADD NEW USER TO AN EVENT
        if event['httpMethod'] == 'POST':
            if 'event_id' in body:
                event_id = body['event_id']
                user_id = body['user_id']
                query = 'INSERT INTO event_users (user_id, event_id) VALUES (%s, %s)'
                cursor.execute(query, (user_id, event_id))
                connection.commit()
                return {
                    'statusCode': 200,
                    'headers': {
                        "Access-Control-Allow-Origin" : "*"
                    },
                    'body': 'Added User'
                }
            # ---------- FOLLOW ANOTHER USER
            if 'follower' in body:
                username = body['user']
                follower = body['follower']
                query = 'SELECT id FROM users WHERE username = %s'
                cursor.execute(query, username)
                result = cursor.fetchone()
                if result is None:
                    return {
                        'statusCode': 400,
                        'headers': {
                            "Access-Control-Allow-Origin" : "*"
                        },
                        'body': 'User does not exist'
                    }
                user = result[0]
                query = 'INSERT INTO friends (user, follower) VALUES (%s, %s)'
                cursor.execute(query, (user, follower))
                connection.commit()
                return {
                    'statusCode': 200,
                    'headers': {
                        "Access-Control-Allow-Origin" : "*"
                    },
                    'body': user
                }
    # ---------- UPDATE EXISTING EVENT
    if event['path'] == '/updateevent' and event['httpMethod'] == 'POST':
        event_id = body['event_id']
        event_name = body['event_name']
        event_date = body['event_date']
        poster_img = body['poster_img']
        
        query = 'UPDATE events SET event_name = %s, event_date = %s, poster_img = %s WHERE id = %s'
        cursor.execute(query, (event_name, event_date, poster_img, event_id))
        connection.commit()
        
        query = 'DELETE FROM event_users WHERE event_id = %s'
        cursor.execute(query, event_id)

        g = body['guests']
        if g:
            guests = g.split(",")
            for guest in guests:
                query = 'INSERT INTO event_users (user_id, event_id) values (%s, %s)'
                cursor.execute(query, (guest, event_id))
                connection.commit()

        return {
            'statusCode': 200,
            'headers': {
                "Access-Control-Allow-Origin" : "*"
            },
            'body': 'Updated event'
        }
    # ---------- GET TOP 5 MOST SELECTED MOVIES
    if event['path'] == '/getrank' and event['httpMethod'] == 'GET':
        query = 'SELECT count(*) as count, movie_name, poster_img from event_users join events on event_users.event_id = events.id group by events.movie_name order by count desc limit 5'
        cursor.execute(query)
        movies = cursor.fetchall()
        return {
            'statusCode': 200,
            'headers': {
                "Access-Control-Allow-Origin" : "*"
            },
            'body': json.dumps(movies, default=str)
        }