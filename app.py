from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import time
import random
from werkzeug.security import generate_password_hash, check_password_hash
import jwt
from datetime import datetime, timedelta
from pymongo import MongoClient
import os

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})
app.config['SECRET_KEY'] = 'your-secret-key-here'
IMAGE_FOLDER = os.path.join(os.path.dirname(__file__), 'engine_images')

# MongoDB setup
client = MongoClient('mongodb://localhost:27017/')
db = client['key_value_store']
users_collection = db['users']
data_collection = db['data']

# B+ Tree Implementation
class BPlusTree:
    def __init__(self, degree=3):
        self.degree = degree
        self.data = {}

    def insert(self, key, value):
        self.data[key] = value

    def get(self, key):
        return self.data.get(key)

    def range_query(self, start_key, end_key):
        return {k: v for k, v in self.data.items() if start_key <= k <= end_key}

    def delete(self, key):
        if key in self.data:
            del self.data[key]

# LSM Tree Implementation
class LSMTree:
    def __init__(self, max_memtable_size=1000):
        self.memtable = {}
        self.max_memtable_size = max_memtable_size
        self.sstables = []

    def insert(self, key, value):
        self.memtable[key] = value
        if len(self.memtable) >= self.max_memtable_size:
            self._flush_to_disk()

    def get(self, key):
        if key in self.memtable:
            return self.memtable[key]
        for sstable in reversed(self.sstables):
            if key in sstable:
                return sstable[key]
        return None

    def range_query(self, start_key, end_key):
        result = {}
        for key, value in self.memtable.items():
            if start_key <= key <= end_key:
                result[key] = value
        for sstable in reversed(self.sstables):
            for key, value in sstable.items():
                if start_key <= key <= end_key and key not in result:
                    result[key] = value
        return result

    def delete(self, key):
        self.memtable[key] = None
        if len(self.memtable) >= self.max_memtable_size:
            self._flush_to_disk()

    def _flush_to_disk(self):
        self.sstables.append(self.memtable.copy())
        self.memtable = {}

# Initialize engines
engines = {
    "bplus": BPlusTree(degree=3),
    "lsm": LSMTree(max_memtable_size=1000)
}
current_engine = "bplus"

@app.route('/engine-image/<engine>')
def get_engine_image(engine):
    try:
        return send_from_directory(IMAGE_FOLDER, f'{engine}_structure.png')
    except FileNotFoundError:
        return jsonify({"error": "Image not found"}), 404

# Authentication routes
@app.route('/signup', methods=['POST'])
def signup():
    data = request.json
    username = data.get('username')
    password = data.get('password')
    
    if not username or not password:
        return jsonify({"error": "Username and password are required"}), 400
    
    if users_collection.find_one({"username": username}):
        return jsonify({"error": "Username already exists"}), 400
    
    hashed_password = generate_password_hash(password)
    users_collection.insert_one({
        "username": username,
        "password": hashed_password,
        "created_at": datetime.utcnow()
    })
    
    return jsonify({"status": "User created successfully"}), 201

@app.route('/login', methods=['POST'])
def login():
    data = request.json
    username = data.get('username')
    password = data.get('password')
    
    if not username or not password:
        return jsonify({"error": "Username and password are required"}), 400
    
    user = users_collection.find_one({"username": username})
    if not user or not check_password_hash(user['password'], password):
        return jsonify({"error": "Invalid username or password"}), 401
    
    token = jwt.encode({
        'username': username,
        'exp': datetime.utcnow() + timedelta(hours=24)
    }, app.config['SECRET_KEY'], algorithm='HS256')
    
    return jsonify({
        "status": "Login successful",
        "token": token,
        "username": username
    })

# Database operations
@app.route('/engine', methods=['POST'])
def switch_engine():
    global current_engine
    data = request.json
    engine = data.get('engine')
    if engine in engines:
        current_engine = engine
        return jsonify({"status": f"Switched to {engine} engine"})
    return jsonify({"error": "Invalid engine"}), 400

@app.route('/insert', methods=['POST'])
def insert():
    data = request.json
    key = data.get('key')
    value = data.get('value')
    
    if not key or not value:
        return jsonify({"error": "Key and value are required"}), 400
    
    try:
        engines[current_engine].insert(str(key), str(value))
        return jsonify({"status": "Insert successful"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/get/<key>', methods=['GET'])
def get(key):
    try:
        value = engines[current_engine].get(str(key))
        if value is None:
            return jsonify({"error": "Key not found"}), 404
        return jsonify({"value": value})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/range/<start_key>/<end_key>', methods=['GET'])
def range_query(start_key, end_key):
    try:
        results = engines[current_engine].range_query(str(start_key), str(end_key))
        return jsonify({"results": results})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/delete/<key>', methods=['DELETE'])
def delete(key):
    try:
        engines[current_engine].delete(str(key))
        return jsonify({"status": "Delete successful"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/benchmark', methods=['GET'])
def benchmark():
    results = {
        "bplus": {"insert": [], "get": [], "range": [], "delete": []},
        "lsm": {"insert": [], "get": [], "range": [], "delete": []}
    }
    
    test_sizes = [10, 50, 100]
    
    for size in test_sizes:
        for name, engine in engines.items():
            try:
                # Reset engine
                if name == "bplus":
                    engines[name] = BPlusTree(degree=3)
                else:
                    engines[name] = LSMTree(max_memtable_size=1000)
                
                # Insert benchmark
                insert_times = []
                for i in range(size):
                    key = str(i)
                    value = f"value_{i}"
                    start = time.perf_counter()
                    engine.insert(key, value)
                    insert_times.append(time.perf_counter() - start)
                
                # Get benchmark
                get_times = []
                sample_keys = random.sample(range(size), min(10, size))
                for i in sample_keys:
                    key = str(i)
                    start = time.perf_counter()
                    val = engine.get(key)
                    get_times.append(time.perf_counter() - start)
                
                # Range query benchmark
                range_times = []
                for i in range(3):
                    start_key = str(i * (size // 3))
                    end_key = str((i + 1) * (size // 3) - 1)
                    start = time.perf_counter()
                    engine.range_query(start_key, end_key)
                    range_times.append(time.perf_counter() - start)
                
                # Delete benchmark
                delete_times = []
                for i in sample_keys[:5]:
                    key = str(i)
                    start = time.perf_counter()
                    engine.delete(key)
                    delete_times.append(time.perf_counter() - start)
                
                # Store results in milliseconds
                results[name]["insert"].append(sum(insert_times)/size * 1000)
                results[name]["get"].append(sum(get_times)/len(get_times) * 1000)
                results[name]["range"].append(sum(range_times)/len(range_times) * 1000)
                results[name]["delete"].append(sum(delete_times)/len(delete_times) * 1000)
                
            except Exception as e:
                return jsonify({"error": f"Benchmark failed for {name} at size {size}: {str(e)}"}), 500
    
    return jsonify({
        "test_sizes": test_sizes,
        "bplus": results["bplus"],
        "lsm": results["lsm"]
    })

if __name__ == '__main__':
    if not os.path.exists(IMAGE_FOLDER):
        os.makedirs(IMAGE_FOLDER)
    app.run(host='0.0.0.0', port=5000, debug=True)