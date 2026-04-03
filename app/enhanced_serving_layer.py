import os
import sys
import json
import time
from datetime import datetime
from functools import wraps
from flask import Flask, jsonify, request, Response
from flask_cors import CORS
from pymongo import MongoClient
from dotenv import load_dotenv

# ==========================================
# 1. CẤU HÌNH HỆ THỐNG (CONFIGURATION)
# ==========================================
load_dotenv()

MONGO_URI = os.environ.get("CONNECTION_STRING", "mongodb://localhost:27017")
SERVER_PORT = int(os.environ.get("SERVING_PORT", "5000"))
CACHE_EXPIRATION_TIME = int(os.environ.get("CACHE_TTL_SECONDS", "60"))
IS_DEBUG_MODE = os.environ.get("DEBUG_MODE", "false").lower() == "true"

print("=" * 80)
print("🎯 LAMBDA ARCHITECTURE - ENHANCED SERVING LAYER")
print("=" * 80)
print(f"📦 Database Connection : {MONGO_URI}")
print(f"🌐 Server Listening on : {SERVER_PORT}")
print(f"⏱️  Cache Expiration    : {CACHE_EXPIRATION_TIME}s")
print(f"🔧 Debug Status        : {IS_DEBUG_MODE}")
print("=" * 80)

# Khởi tạo Flask App
app = Flask(__name__)
CORS(app)

# Kết nối MongoDB
mongo_client = MongoClient(MONGO_URI)
mongodb_database = mongo_client['BIGDATA']

# Cấu trúc bộ nhớ đệm (In-memory Cache)
data_cache_store = {}
cache_expiry_registry = {}