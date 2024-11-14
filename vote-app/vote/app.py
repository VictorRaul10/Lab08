from flask import Flask, render_template, request, make_response, g, jsonify
from redis import Redis
import os
import socket
import random
import json
import logging
from confluent_kafka import Producer, Consumer, KafkaException
import time
import threading
import pyodbc  # Importar pyodbc para la conexión a la base de datos

# Configuración de opciones
option_a = os.getenv('OPTION_A', "Cats")
option_b = os.getenv('OPTION_B', "Dogs")
option_c = os.getenv('OPTION_C', "Rabbits")
hostname = socket.gethostname()

# Configuración de la conexión a SQL Server
server = 'ec2-44-204-145-138.compute-1.amazonaws.com'
database = 'movielens'
username = 'SA'
password = 'YourStrong@Passw0rd'

conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"

# Configuración del productor de Kafka
kafka_bootstrap_servers = 'ec2-18-206-252-137.compute-1.amazonaws.com:29092'
kafka_producer = Producer({'bootstrap.servers': kafka_bootstrap_servers})

# Variable global para controlar la producción de datos
producing_data = False

def send_kafka_message(message):
    kafka_producer.produce('data_topic', key='data', value=message)
    kafka_producer.flush()

def produce_random_data():
    global producing_data
    while producing_data:
        random_data = f"Random data: {random.randint(1, 100)}"
        send_kafka_message(random_data)
        time.sleep(3)  # Espera 3 segundos entre envíos

# Configuración del consumidor de Kafka
def get_kafka_consumer():
    consumer = Consumer({
        'bootstrap.servers': kafka_bootstrap_servers,
        'group.id': 'flask-consumer',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe(['data_topic'])
    return consumer

app = Flask(__name__)

# Para capturar errores de gunicorn
gunicorn_error_logger = logging.getLogger('gunicorn.error')
app.logger.handlers.extend(gunicorn_error_logger.handlers)
app.logger.setLevel(logging.INFO)

def get_redis():
    if not hasattr(g, 'redis'):
        g.redis = Redis(host="redis", db=0, socket_timeout=5)
    return g.redis

def get_db_connection():
    conn = pyodbc.connect(conn_str)
    return conn

def get_movie_ratings(user1_id, user2_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    query = """
    SELECT r.user_id, r.movie_id, r.rating, m.title
    FROM ratings r
    JOIN movies m ON r.movie_id = m.movie_id
    WHERE r.user_id IN (?, ?)
    """
    cursor.execute(query, (user1_id, user2_id))
    ratings = cursor.fetchall()
    cursor.close()
    conn.close()
    return ratings

def manhattan_distance(user1_ratings, user2_ratings):
    distance = 0
    for movie_id in set(user1_ratings.keys()).intersection(set(user2_ratings.keys())):
        rating1 = user1_ratings.get(movie_id, 0)
        rating2 = user2_ratings.get(movie_id, 0)
        distance += abs(rating1 - rating2)
    return distance

def get_best_movies(user1_ratings, user2_ratings, movie_titles, top_n=3):
    movie_scores = []
    
    for movie_id in set(user1_ratings.keys()).intersection(set(user2_ratings.keys())):
        rating1 = user1_ratings[movie_id]
        rating2 = user2_ratings[movie_id]
        movie_scores.append((movie_id, rating1, rating2))

    distances = []
    for movie_id, rating1, rating2 in movie_scores:
        distance = abs(rating1 - rating2)
        distances.append((movie_id, distance))

    distances.sort(key=lambda x: x[1])
    best_movies = [(movie_id, movie_titles[movie_id]) for movie_id, _ in distances[:top_n]]
    return best_movies

@app.route("/", methods=['POST', 'GET'])
def hello():
    global producing_data
    voter_id = request.cookies.get('voter_id')
    if not voter_id:
        voter_id = hex(random.getrandbits(64))[2:-1]

    vote = None

    if request.method == 'POST':
        redis = get_redis()
        vote = request.form['vote']
        app.logger.info('Received vote for %s', vote)
        data = json.dumps({'voter_id': voter_id, 'vote': vote})
        redis.rpush('votes', data)

    # Aquí puedes establecer los IDs de los usuarios de los que deseas obtener las mejores películas
    user1_id = 1  # Cambia al ID de usuario correspondiente
    user2_id = 2  # Cambia al ID de usuario correspondiente
    ratings = get_movie_ratings(user1_id, user2_id)
    
    user1_ratings = {}
    user2_ratings = {}
    movie_titles = {}

    for row in ratings:
        user_id, movie_id, rating, title = row
        if user_id == user1_id:
            user1_ratings[movie_id] = rating
        else:
            user2_ratings[movie_id] = rating
        movie_titles[movie_id] = title

    distance = manhattan_distance(user1_ratings, user2_ratings)
    best_movies = get_best_movies(user1_ratings, user2_ratings, movie_titles)

    resp = make_response(render_template(
        'index.html',
        option_a=option_a,
        option_b=option_b,
        option_c=option_c,
        hostname=hostname,
        vote=vote,
        distance=distance,
        best_movies=best_movies  # Pasar las mejores películas a la plantilla
    ))
    resp.set_cookie('voter_id', voter_id)
    return resp


@app.route('/start-kafka', methods=['POST'])
def start_kafka_data():
    global producing_data
    producing_data = True
    threading.Thread(target=produce_random_data).start()  # Inicia el hilo para producir datos
    return jsonify(status="Started producing random data")

@app.route('/stop-kafka', methods=['POST'])
def stop_kafka_data():
    global producing_data
    producing_data = False
    return jsonify(status="Stopped producing random data")

@app.route("/kafka-data", methods=['GET'])
def get_kafka_data():
    consumer = get_kafka_consumer()
    messages = []
    for _ in range(10):  # Obtener datos
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        messages.append(msg.value().decode('utf-8'))
    consumer.close()  # Cerrar el consumidor después de obtener los datos
    return jsonify(messages)

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=80, debug=True, threaded=True)
