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

def cached(ttl_seconds=None):
    """Decorator quản lý bộ nhớ đệm theo thời gian (TTL)"""
    def decorator(target_function):
        @wraps(target_function)
        def wrapper(*args, **kwargs):
            ttl = ttl_seconds or CACHE_EXPIRATION_TIME
            # Tạo key dựa trên tên hàm và tham số truyền vào
            cache_identifier = f"{target_function.__name__}:{str(args)}:{str(sorted(kwargs.items()))}"
            
            # Kiểm tra dữ liệu trong cache còn hạn không
            if cache_identifier in data_cache_store:
                last_cached_time = cache_expiry_registry.get(cache_identifier, 0)
                if time.time() - last_cached_time < ttl:
                    return data_cache_store[cache_identifier]
            
            # Nếu không có hoặc hết hạn, thực thi hàm và lưu lại
            execution_result = target_function(*args, **kwargs)
            data_cache_store[cache_identifier] = execution_result
            cache_expiry_registry[cache_identifier] = time.time()
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
# 2. HÀM HỢP NHẤT DỮ LIỆU
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