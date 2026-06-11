import os
import sys
import json
import time
import threading
from collections import OrderedDict
from datetime import datetime
from functools import wraps
from flask import Flask, jsonify, request, Response
from flask_cors import CORS
from pymongo import MongoClient
from dotenv import load_dotenv

# ==========================================
# CẤU HÌNH HỆ THỐNG (CONFIGURATION)
# ==========================================
load_dotenv()

MONGO_URI = os.environ.get("CONNECTION_STRING", "mongodb://localhost:27017")
SERVER_PORT = int(os.environ.get("SERVING_PORT", "5000"))
CACHE_EXPIRATION_TIME = int(os.environ.get("CACHE_TTL_SECONDS", "60"))
CACHE_MAX_SIZE = int(os.environ.get("CACHE_MAX_SIZE", "1000"))
IS_DEBUG_MODE = os.environ.get("DEBUG_MODE", "false").lower() == "true"
ES_NODES = os.environ.get("ES_NODES", "localhost")
ES_PORT = os.environ.get("ES_PORT", "9200")

print("=" * 80)
print("🎯 LAMBDA ARCHITECTURE - ENHANCED SERVING LAYER")
print("=" * 80)
print(f"📦 Database Connection : {MONGO_URI}")
print(f"🌐 Server Listening on : {SERVER_PORT}")
print(f"⏱️  Cache Expiration    : {CACHE_EXPIRATION_TIME}s")
print(f"🔍 Elasticsearch       : {ES_NODES}:{ES_PORT}")
print(f"🔧 Debug Status        : {IS_DEBUG_MODE}")
print("=" * 80)

# Khởi tạo Flask App
app = Flask(__name__)
CORS(app)

# Kết nối MongoDB
mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
mongodb_database = mongo_client['BIGDATA']

# Kết nối Elasticsearch (graceful fallback nếu ES không khả dụng)
es_client = None
try:
    from elasticsearch import Elasticsearch
    es_client = Elasticsearch(f"http://{ES_NODES}:{ES_PORT}", request_timeout=5)
    if es_client.ping():
        print("✅ Elasticsearch connected")
    else:
        print("⚠️  Elasticsearch ping failed, falling back to MongoDB search")
        es_client = None
except Exception as e:
    print(f"⚠️  Elasticsearch unavailable ({e}), falling back to MongoDB search")
    es_client = None

# Cấu trúc bộ nhớ đệm (Thread-safe, Size-bounded Cache)
_cache_lock = threading.Lock()
data_cache_store = OrderedDict()
cache_expiry_registry = {}

def cached(ttl_seconds=None):
    """Decorator quản lý bộ nhớ đệm theo thời gian (TTL) - thread-safe, size-bounded.
    Includes request.args in cache key so different query params are cached separately."""
    def decorator(target_function):
        @wraps(target_function)
        def wrapper(*args, **kwargs):
            ttl = ttl_seconds or CACHE_EXPIRATION_TIME
            # Tạo key dựa trên tên hàm, query params (request.args), và tham số truyền vào
            try:
                query_string = str(sorted(request.args.items()))
            except RuntimeError:
                query_string = ""
            cache_identifier = f"{target_function.__name__}:{query_string}:{str(args)}:{str(sorted(kwargs.items()))}"
            
            with _cache_lock:
                # Kiểm tra dữ liệu trong cache còn hạn không
                if cache_identifier in data_cache_store:
                    last_cached_time = cache_expiry_registry.get(cache_identifier, 0)
                    if time.time() - last_cached_time < ttl:
                        # Move to end (most recently accessed)
                        data_cache_store.move_to_end(cache_identifier)
                        return data_cache_store[cache_identifier]
            
            # Nếu không có hoặc hết hạn, thực thi hàm và lưu lại
            execution_result = target_function(*args, **kwargs)
            
            with _cache_lock:
                data_cache_store[cache_identifier] = execution_result
                cache_expiry_registry[cache_identifier] = time.time()
                # Evict oldest entries when cache exceeds max size
                while len(data_cache_store) > CACHE_MAX_SIZE:
                    oldest_key, _ = data_cache_store.popitem(last=False)
                    cache_expiry_registry.pop(oldest_key, None)
            
            return execution_result
        return wrapper
    return decorator


def build_api_response(content, metadata=None):
    """Chuẩn hóa cấu trúc phản hồi API"""
    response_body = {
        "success": True,
        "data": content,
        "timestamp": datetime.now().isoformat(),
        "layer": "serving_layer"
    }
    if metadata:
        response_body["meta"] = metadata
    return response_body

def apply_pagination(data_list, current_page=1, items_per_page=20):
    """Hàm phân trang cho danh sách dữ liệu"""
    start_index = (current_page - 1) * items_per_page
    end_index = start_index + items_per_page
    
    return {
        "items": data_list[start_index:end_index],
        "total_records": len(data_list),
        "current_page": current_page,
        "per_page": items_per_page,
        "total_pages": (len(data_list) + items_per_page - 1) // items_per_page
    }

#==========================================
# HÀM HỢP NHẤT DỮ LIỆU
#==========================================
def merge_batch_and_speed_records(batch_records, speed_records, unique_key):
    """
    Hợp nhất bản ghi từ Batch Layer và Speed Layer.
    Speed data có độ ưu tiên cao hơn (ghi đè) nếu trùng khóa.
    """
    combined_map = {}
    
    # Nạp dữ liệu từ Batch Layer trước
    for record in batch_records:
        record_id = str(record.get(unique_key))
        if record_id:
            combined_map[record_id] = record.copy()
            combined_map[record_id]['source_layer'] = 'batch'
            combined_map[record_id]['_mongo_id'] = str(record.get('_id', ''))
    
    # Ghi đè hoặc thêm mới từ Speed Layer (Dữ liệu thời gian thực)
    for record in speed_records:
        record_id = str(record.get(unique_key))
        if record_id:
            if record_id in combined_map:
                # Cập nhật các trường dữ liệu mới từ speed layer
                for field_name, field_value in record.items():
                    if field_value is not None:
                        combined_map[record_id][field_name] = field_value
                combined_map[record_id]['source_layer'] = 'merged'
            else:
                combined_map[record_id] = record.copy()
                combined_map[record_id]['source_layer'] = 'speed'
            combined_map[record_id]['_mongo_id'] = str(record.get('_id', ''))
    
    final_list = list(combined_map.values())
    # Loại bỏ ObjectId của MongoDB để tránh lỗi JSON serialize
    for item in final_list:
        if '_id' in item: del item['_id']
    
    return final_list


def merge_aggregated_stats(batch_agg_data, speed_agg_data, group_by_field, sum_fields=None, weight_avg_fields=None):
    """
    Hợp nhất dữ liệu thống kê từ 2 layer.
    - sum_fields: Các trường cần cộng dồn (ví dụ: doanh thu).
    - weight_avg_fields: Các trường cần tính lại trung bình có trọng số (ví dụ: điểm đánh giá).
    """
    sum_fields = sum_fields or []
    weight_avg_fields = weight_avg_fields or []
    merged_stats = {}
    
    # Xử lý Batch Aggregations
    for entry in batch_agg_data:
        key = str(entry.get(group_by_field))
        if key:
            merged_stats[key] = entry.copy()
            merged_stats[key]['batch_count'] = entry.get('movie_count', 0)
            merged_stats[key]['_mongo_id'] = str(entry.get('_id', ''))
    
    # Cập nhật Incremental từ Speed Layer
    for entry in speed_agg_data:
        key = str(entry.get(group_by_field))
        if key:
            if key in merged_stats:
                b_count = merged_stats[key].get('batch_count', 0)
                s_count = entry.get('movie_count', 0)
                total_count = b_count + s_count
                
                merged_stats[key]['movie_count'] = total_count
                
                # Tính trung bình Weighted Average
                for field in weight_avg_fields:
                    if b_count > 0 and s_count > 0:
                        b_val = merged_stats[key].get(field, 0) or 0
                        s_val = entry.get(field, 0) or 0
                        merged_stats[key][field] = round(
                            (b_val * b_count + s_val * s_count) / total_count, 2
                        )
                
                for field in sum_fields:
                    if field in entry:
                        current_total = merged_stats[key].get(field, 0) or 0
                        incremental_val = entry.get(field, 0) or 0
                        merged_stats[key][field] = current_total + incremental_val
                
                merged_stats[key]['source_layer'] = 'merged'
                merged_stats[key]['speed_count'] = s_count
            else:
                merged_stats[key] = entry.copy()
                merged_stats[key]['source_layer'] = 'speed'
            merged_stats[key]['_mongo_id'] = str(entry.get('_id', ''))
    
    result_list = list(merged_stats.values())
    for item in result_list:
        if '_id' in item: del item['_id']
    
    return result_list


#==========================================
# API ENDPOINTS
#==========================================
@app.route('/')
def api_documentation():
    """Trang hướng dẫn sử dụng API"""
    return jsonify({
        "service": "Lambda Architecture - Enhanced Serving Layer",
        "version": "3.0",
        "endpoints": {
            "movies": ["/api/movies", "/api/movies/search", "/api/movies/<id>"],
            "actors": ["/api/actors", "/api/actors/search", "/api/actors/<id>"],
            "statistics": ["/api/stats/genres", "/api/stats/years", "/api/stats/languages", "/api/stats/directors"],
            "rankings": ["/api/top/movies", "/api/top/profitable", "/api/top/popular"],
            "analytics": ["/api/analytics/decade-rating", "/api/analytics/quarter-budget", "/api/analytics/year-genre", "/api/analytics/trends"],
            "monitoring": ["/api/lambda/status", "/api/health", "/api/metrics"]
        },
        "search_backend": "elasticsearch" if es_client else "mongodb_regex"
    })


@app.route('/api/health')
def health_check():
    """Kiểm tra trạng thái kết nối DB"""
    health_status = {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "components": {}
    }
    try:
        mongo_client.admin.command('ping')
        health_status["components"]["mongodb"] = {"status": "connected", "db": "BIGDATA"}
    except Exception as error:
        health_status["components"]["mongodb"] = {"status": "error", "error": str(error)}
        health_status["status"] = "degraded"
    return jsonify(health_status)


@app.route('/api/metrics')
def system_metrics():
    """Thống kê số lượng bản ghi các collection"""
    try:
        coll_stats = {name: {"count": mongodb_database[name].count_documents({})} 
                      for name in mongodb_database.list_collection_names()}
        return jsonify({
            "timestamp": datetime.now().isoformat(),
            "collections": coll_stats,
            "cache_status": {"items_cached": len(data_cache_store), "ttl": CACHE_EXPIRATION_TIME}
        })
    except Exception as error:
        return jsonify({"error": str(error)}), 500
    
    #=====================================
    #API movies
    #=====================================
    
@app.route('/api/movies')
def get_movies_list():
    """Lấy danh sách phim kết hợp từ Batch và Speed Layer"""
    limit_val = int(request.args.get('limit', 20))
    page_val = int(request.args.get('page', 1))
    sort_field = request.args.get('sort_by', 'popularity')
    sort_order = -1 if request.args.get('order', 'desc') == 'desc' else 1
    
    # Bộ lọc
    filter_query = {}
    if request.args.get('year'): filter_query['release_year'] = int(request.args.get('year'))
    if request.args.get('genre'): filter_query['genres'] = {'$regex': request.args.get('genre'), '$options': 'i'}
    if request.args.get('language'): filter_query['original_language'] = request.args.get('language')
    
    try:
        batch_data = list(mongodb_database['batch_movies'].find(filter_query, {'_id': 0})
                          .sort(sort_field, sort_order).limit(limit_val * 2))
        
        speed_data = list(mongodb_database['speed_movies'].find(filter_query, {'_id': 0})
                          .sort('processed_at', -1).limit(limit_val))
        
        merged_results = merge_batch_and_speed_records(batch_data, speed_data, 'id')
        merged_results.sort(key=lambda x: x.get(sort_field, 0) or 0, reverse=(sort_order == -1))
        
        paginated_data = apply_pagination(merged_results, page_val, limit_val)
        
        return jsonify(build_api_response(
            paginated_data["items"],
            {
                "pagination": paginated_data,
                "data_sources": {"batch_raw_count": len(batch_data), "speed_raw_count": len(speed_data)}
            }
        ))
    except Exception as error:
        return jsonify({"success": False, "error": str(error)}), 500


@app.route('/api/movies/search')
def search_movies_db():
    """Tìm kiếm phim theo từ khóa - Elasticsearch primary, MongoDB fallback"""
    search_term = request.args.get('q', '')
    limit_val = int(request.args.get('limit', 20))
    
    if not search_term:
        return jsonify({"success": False, "error": "Missing 'q' parameter"}), 400
    
    search_backend = "mongodb_regex"  # default
    
    # Primary path: Elasticsearch full-text search
    if es_client:
        try:
            es_query = {
                "query": {
                    "multi_match": {
                        "query": search_term,
                        "fields": ["title^3", "overview", "genres"],
                        "type": "best_fields",
                        "fuzziness": "AUTO"
                    }
                },
                "size": limit_val
            }
            
            batch_hits = []
            speed_hits = []
            
            # Search batch-movie index
            try:
                batch_resp = es_client.search(index="batch-movie", body=es_query)
                batch_hits = [hit["_source"] for hit in batch_resp["hits"]["hits"]]
            except Exception:
                pass  # index may not exist yet
            
            # Search speed-movie index
            try:
                speed_resp = es_client.search(index="speed-movie", body=es_query)
                speed_hits = [hit["_source"] for hit in speed_resp["hits"]["hits"]]
            except Exception:
                pass  # index may not exist yet
            
            if batch_hits or speed_hits:
                merged = merge_batch_and_speed_records(batch_hits, speed_hits, 'id')
                search_backend = "elasticsearch"
                return jsonify(build_api_response(
                    merged[:limit_val],
                    {"query": search_term, "search_backend": search_backend}
                ))
        except Exception as es_error:
            # ES failed, fall through to MongoDB fallback
            print(f"Elasticsearch search error: {es_error}")
    
    # Fallback path: MongoDB regex search
    try:
        regex_filter = {"$regex": search_term, "$options": "i"}
        batch_search = list(mongodb_database['batch_movies'].find({
            "$or": [{"title": regex_filter}, {"overview": regex_filter}, {"genres": regex_filter}]
        }, {'_id': 0}).limit(limit_val))
        
        speed_search = list(mongodb_database['speed_movies'].find({
            "$or": [{"title": regex_filter}, {"genres_flat": regex_filter}]
        }, {'_id': 0}).limit(limit_val))
        
        merged = merge_batch_and_speed_records(batch_search, speed_search, 'id')
        return jsonify(build_api_response(
            merged[:limit_val],
            {"query": search_term, "search_backend": search_backend}
        ))
    except Exception as error:
        return jsonify({"success": False, "error": str(error)}), 500


@app.route('/api/movies/<movie_id>')
def get_single_movie(movie_id):
    """Chi tiết một bộ phim theo ID"""
    try:
        # Query with both int and string formats to handle type mismatch
        # between batch layer (int id) and speed layer (string id)
        id_query = {'$or': [{'id': movie_id}]}
        try:
            id_query['$or'].append({'id': int(movie_id)})
        except (ValueError, TypeError):
            pass  # non-numeric ID, skip int variant
        
        batch_record = mongodb_database['batch_movies'].find_one(id_query, {'_id': 0})
        speed_record = mongodb_database['speed_movies'].find_one(id_query, {'_id': 0})
        
        if batch_record and speed_record:
            result = {**batch_record, **speed_record, 'source_layer': 'merged'}
        elif speed_record:
            result = {**speed_record, 'source_layer': 'speed'}
        elif batch_record:
            result = {**batch_record, 'source_layer': 'batch'}
        else:
            return jsonify({"success": False, "error": "Movie not found"}), 404
            
        return jsonify(build_api_response(result))
    except Exception as error:
        return jsonify({"success": False, "error": str(error)}), 500
    
#===========================================
#API statistics
#===========================================
@app.route('/api/stats/genres')
@cached(ttl_seconds=60)
def get_genre_statistics():
    """Thống kê theo thể loại"""
    try:
        batch_stats = list(mongodb_database['batch_genre_stats'].find({}, {'_id': 0}))
        speed_stats = list(mongodb_database['speed_genre_stats'].find({}, {'_id': 0}))
        
        merged = merge_aggregated_stats(
            batch_stats, speed_stats, 'genre',
            sum_fields=['total_revenue', 'total_budget', 'profitable_count'],
            weight_avg_fields=['avg_rating', 'avg_popularity']
        )
        merged.sort(key=lambda x: x.get('movie_count', 0), reverse=True)
        return jsonify(build_api_response(merged))
    except Exception as error:
        return jsonify({"success": False, "error": str(error)}), 500


@app.route('/api/stats/years')
@cached(ttl_seconds=60)
def get_year_statistics():
    """Thống kê theo năm phát hành"""
    try:
        batch_stats = list(mongodb_database['batch_year_stats'].find({}, {'_id': 0}))
        speed_stats = list(mongodb_database['speed_year_stats'].find({}, {'_id': 0}))
        
        merged = merge_aggregated_stats(
            batch_stats, speed_stats, 'release_year',
            sum_fields=['total_revenue', 'total_budget', 'profitable_count'],
            weight_avg_fields=['avg_rating', 'avg_popularity']
        )
        merged.sort(key=lambda x: x.get('release_year', 0), reverse=True)
        return jsonify(build_api_response(merged))
    except Exception as error:
        return jsonify({"success": False, "error": str(error)}), 500


@app.route('/api/stats/languages')
@cached(ttl_seconds=60)
def get_language_statistics():
    """Thống kê theo ngôn ngữ"""
    try:
        batch_stats = list(mongodb_database['batch_language_stats'].find({}, {'_id': 0}))
        speed_stats = list(mongodb_database['speed_language_stats'].find({}, {'_id': 0}))
        
        merged = merge_aggregated_stats(
            batch_stats, speed_stats, 'original_language',
            sum_fields=['total_revenue'],
            weight_avg_fields=['avg_rating', 'avg_popularity']
        )
        merged.sort(key=lambda x: x.get('movie_count', 0), reverse=True)
        return jsonify(build_api_response(merged))
    except Exception as error:
        return jsonify({"success": False, "error": str(error)}), 500


@app.route('/api/stats/directors')
@cached(ttl_seconds=120)
def get_director_statistics():
    """Thống kê đạo diễn từ Batch Layer"""
    limit_val = int(request.args.get('limit', 50))
    tier_filter = request.args.get('tier')
    
    try:
        query = {'director_tier': tier_filter} if tier_filter else {}
        stats = list(mongodb_database['batch_director_stats'].find(query, {'_id': 0})
                     .sort('movie_count', -1).limit(limit_val))
        return jsonify(build_api_response(stats, {"tiers": ["Top 25%", "Top 50%", "Top 75%", "Bottom 25%"]}))
    except Exception as error:
        return jsonify({"success": False, "error": str(error)}), 500
    
    #===========================================
    #API ranking
    #===========================================
@app.route('/api/top/movies')
@cached(ttl_seconds=60)
def get_top_rated_movies():
    """Top phim điểm cao nhất"""
    limit_val = int(request.args.get('limit', 20))
    sort_by = request.args.get('sort_by', 'vote_average')
    
    try:
        batch_top = list(mongodb_database['batch_top_movies'].find({}, {'_id': 0}).sort(sort_by, -1).limit(limit_val))
        speed_top = list(mongodb_database['speed_top_movies'].find({}, {'_id': 0}).sort('popularity', -1).limit(limit_val))
        
        merged = merge_batch_and_speed_records(batch_top, speed_top, 'id')
        merged.sort(key=lambda x: x.get(sort_by, 0) or 0, reverse=True)
        return jsonify(build_api_response(merged[:limit_val]))
    except Exception as error:
        return jsonify({"success": False, "error": str(error)}), 500


@app.route('/api/top/profitable')
@cached(ttl_seconds=120)
def get_most_profitable():
    """Top phim sinh lời cao nhất (ROI)"""
    limit_val = int(request.args.get('limit', 20))
    try:
        data = list(mongodb_database['batch_top_movies'].find({'is_profitable': True}, {'_id': 0})
                    .sort('roi_percent', -1).limit(limit_val))
        return jsonify(build_api_response(data))
    except Exception as error:
        return jsonify({"success": False, "error": str(error)}), 500


@app.route('/api/top/popular')
@cached(ttl_seconds=60)
def get_most_popular():
    """Top phim phổ biến (Ưu tiên Real-time từ Speed Layer)"""
    limit_val = int(request.args.get('limit', 20))
    try:
        speed_data = list(mongodb_database['speed_top_movies'].find({}, {'_id': 0}).sort('popularity', -1).limit(limit_val))
        if len(speed_data) < limit_val:
            batch_data = list(mongodb_database['batch_top_movies'].find({}, {'_id': 0}).sort('popularity', -1).limit(limit_val))
            merged = merge_batch_and_speed_records(batch_data, speed_data, 'id')
            merged.sort(key=lambda x: x.get('popularity', 0) or 0, reverse=True)
            final = merged[:limit_val]
        else:
            final = speed_data
        return jsonify(build_api_response(final, {"source": "speed_layer_prioritized"}))
    except Exception as error:
        return jsonify({"success": False, "error": str(error)}), 500
    
#===========================================
#API analytics
#===========================================
@app.route('/api/analytics/decade-rating')
@cached(ttl_seconds=300)
def analytics_decade_rating():
    """Bảng chuyển trục Thập kỷ x Xếp hạng"""
    try:
        data = list(mongodb_database['batch_decade_rating_pivot'].find({}, {'_id': 0}))
        return jsonify(build_api_response(data, {"type": "pivot", "metric": "movie_count"}))
    except Exception as error:
        return jsonify({"success": False, "error": str(error)}), 500


@app.route('/api/analytics/quarter-budget')
@cached(ttl_seconds=300)
def analytics_quarter_budget():
    """Bảng chuyển trục Quý x Ngân sách"""
    try:
        data = list(mongodb_database['batch_quarter_budget_pivot'].find({}, {'_id': 0}))
        return jsonify(build_api_response(data, {"type": "pivot", "metric": "avg_revenue"}))
    except Exception as error:
        return jsonify({"success": False, "error": str(error)}), 500


@app.route('/api/analytics/year-genre')
@cached(ttl_seconds=300)
def analytics_year_genre():
    """Bảng chuyển trục Năm x Thể loại"""
    try:
        data = list(mongodb_database['batch_year_genre_pivot'].find({}, {'_id': 0}))
        return jsonify(build_api_response(data))
    except Exception as error:
        return jsonify({"success": False, "error": str(error)}), 500


@app.route('/api/analytics/trends')
@cached(ttl_seconds=60)
def get_market_trends():
    """Phân tích xu hướng tăng trưởng YoY"""
    try:
        year_data = list(mongodb_database['batch_year_stats'].find(
            {'yoy_movie_growth': {'$exists': True}}, {'_id': 0}
        ).sort('release_year', -1).limit(20))
        
        if year_data:
            avg_growth = sum([y.get('yoy_movie_growth', 0) or 0 for y in year_data]) / len(year_data)
        else:
            avg_growth = 0
            
        return jsonify(build_api_response({
            "trends": year_data,
            "summary": {"avg_yoy_growth_percent": round(avg_growth, 2)}
        }))
    except Exception as error:
        return jsonify({"success": False, "error": str(error)}), 500

#===========================================
#lambda status
#=============================================

@app.route('/api/lambda/status')
def get_lambda_architecture_status():
    """Tổng quan trạng thái toàn bộ hệ thống Lambda"""
    try:
        # Trạng thái Batch Layer
        batch_count = mongodb_database['batch_movies'].count_documents({})
        latest_batch = mongodb_database['batch_movies'].find_one({}, sort=[('batch_processed_at', -1)])
        
        # Trạng thái Speed Layer
        speed_count = mongodb_database['speed_movies'].count_documents({})
        latest_speed = mongodb_database['speed_movies'].find_one({}, sort=[('processed_at', -1)])

        status_report = {
            "timestamp": datetime.now().isoformat(),
            "layers": {
                "batch_layer": {
                    "status": "active" if batch_count > 0 else "inactive",
                    "total_records": batch_count,
                    "last_run": str(latest_batch.get('batch_processed_at')) if latest_batch else None
                },
                "speed_layer": {
                    "status": "streaming" if speed_count > 0 else "waiting",
                    "total_records": speed_count,
                    "last_event": str(latest_speed.get('processed_at')) if latest_speed else None
                },
                "serving_layer": {
                    "cache_items": len(data_cache_store),
                    "connection": "stable"
                }
            }
        }
        return jsonify(status_report)
    except Exception as error:
        return jsonify({"error": str(error)}), 500


@app.route('/api/lambda/collections')
def list_all_collections():
    """Liệt kê chi tiết các bảng dữ liệu"""
    try:
        colls = {}
        for name in mongodb_database.list_collection_names():
            colls[name] = {
                "count": mongodb_database[name].count_documents({}),
                "type": "batch" if "batch" in name else "speed" if "speed" in name else "other"
            }
        return jsonify(build_api_response(colls))
    except Exception as error:
        return jsonify({"error": str(error)}), 500


#===========================================
#  CHẠY SERVER
#===========================================


@app.route('/api/raw/genres')
def metabase_raw_genres():
    data = list(mongodb_database['batch_genre_stats'].find({}, {'_id': 0}))
    return Response(json.dumps(data), mimetype='application/json')

@app.route('/api/raw/years')
def metabase_raw_years():
    data = list(mongodb_database['batch_year_stats'].find({}, {'_id': 0}).sort('release_year', 1))
    return Response(json.dumps(data), mimetype='application/json')

@app.route('/api/raw/movies')
def metabase_raw_movies():
    limit_val = int(request.args.get('limit', 1000))
    data = list(mongodb_database['batch_movies'].find({}, {'_id': 0}).limit(limit_val))
    return Response(json.dumps(data), mimetype='application/json')


# ==========================================
#  ERROR & main
# ==========================================

@app.errorhandler(404)
def handle_404(e):
    return jsonify({"success": False, "error": "Endpoint not found"}), 404

@app.errorhandler(500)
def handle_500(e):
    return jsonify({"success": False, "error": "Internal server error", "details": str(e)}), 500

def start_server():
    """Khởi chạy Serving Layer"""
    print(f"\n🚀 Serving Layer đang chạy tại: http://0.0.0.0:{SERVER_PORT}")
    print(f"📖 Tài liệu hướng dẫn API: http://0.0.0.0:{SERVER_PORT}/\n")
    app.run(host='0.0.0.0', port=SERVER_PORT, debug=IS_DEBUG_MODE, threaded=True)



if __name__ == "__main__":
    start_server()


