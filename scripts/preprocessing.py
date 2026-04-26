import os
import joblib
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder, StandardScaler

def load_and_merge_data(data_path):
    """Đọc dữ liệu từ file csv."""
    return pd.read_csv(data_path)

def clean_data(df):
    """Làm sạch dữ liệu: xử lý sold_count và điền giá trị khuyết."""
    temp_df = df.copy()
    
    # 1. Xử lý sold_count (VD: 'Đã bán 1.2k' -> 1200)
    def parse_sold_count(value):
        if pd.isna(value): return 0
        val_str = str(value).lower().replace('đã bán', '').replace('.', '').replace(',', '').strip()
        if 'k' in val_str:
            return float(val_str.replace('k', '')) * 1000
        return float(val_str) if val_str != '' else 0

    temp_df['sold_count'] = temp_df['sold_count'].apply(parse_sold_count)
    
    # 2. Xử lý giá trị khuyết
    temp_df['price_original'] = temp_df['price_original'].fillna(temp_df['price_current'])
    
    cols_to_fix = ['rating', 'review_count', 'image_count']
    for col in cols_to_fix:
        if col in temp_df.columns:
            temp_df[col] = pd.to_numeric(temp_df[col], errors='coerce').fillna(0)
            
    # 3. Ép kiểu dữ liệu
    temp_df['crawled_at'] = pd.to_datetime(temp_df['crawled_at'])
    if 'is_mall' in temp_df.columns:
        temp_df['is_mall'] = temp_df['is_mall'].astype(bool).astype(int)
            
    return temp_df

def feature_engineering(df):
    """Trích xuất Brand, tính Discount và tạo các biến mục tiêu Log."""
    df_fe = df.copy()
    
    # Trích xuất Brand
    df_fe['brand'] = df_fe['product_name'].apply(lambda x: str(x).split()[0].upper())
    
    # Tính toán actual_discount (Chỉ dùng làm Feature cho bài toán Sales)
    df_fe['actual_discount'] = (df_fe['price_original'] - df_fe['price_current']) / df_fe['price_original']
    
    # Trích xuất thời gian
    df_fe['day_of_week'] = df_fe['crawled_at'].dt.dayofweek
    
    # Tạo biến mục tiêu (Target)
    df_fe['log_price'] = np.log1p(df_fe['price_current'])
    df_fe['log_sold_count'] = np.log1p(df_fe['sold_count'])
    
    return df_fe

def handle_outliers(df, column):
    """Giới hạn ngoại lai bằng IQR Clipping (Phiên bản an toàn)"""
    # TẠO BẢN SAO ĐỂ TRÁNH THAY ĐỔI DỮ LIỆU GỐC
    df_out = df.copy() 
    
    # Nếu truyền vào một chuỗi (tên cột), chuyển nó thành list để xử lý đồng nhất
    if isinstance(column, str):
        column = [column]
        
    for col in column:
        if col in df_out.columns:
            Q1 = df_out[col].quantile(0.25)
            Q3 = df_out[col].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            # Clipping
            df_out[col] = df_out[col].clip(lower=lower_bound, upper=upper_bound)
            
    return df_out

def final_preprocessing_pipeline(df, task='price', save_path='../data/model/'):
    """
    Pipeline xử lý dữ liệu cho từng bài toán cụ thể:
    - task='price': Loại bỏ discount để tránh rò rỉ dữ liệu.
    - task='sales': Giữ lại giá và discount làm đặc trưng đầu vào.
    """
    if task == 'price':
        target = 'log_price'
        # KHÔNG bao gồm actual_discount
        features = ['brand', 'category_id', 'shop_id', 'is_mall', 'price_original', 
                    'rating', 'review_count', 'day_of_week']
        num_scale = ['price_original', 'review_count']
    else:
        target = 'log_sold_count'
        # BAO GỒM giá hiện tại và discount để dự báo sức mua
        features = ['brand', 'category_id', 'shop_id', 'is_mall', 'price_current', 
                    'rating', 'review_count', 'actual_discount', 'day_of_week']
        num_scale = ['price_current', 'review_count', 'actual_discount']
    
    X = df[features].copy()
    y = df[target]
    
    # 1. Label Encoding cho các biến định danh
    le = LabelEncoder()
    for col in ['brand', 'category_id', 'shop_id']:
        X[col] = le.fit_transform(X[col].astype(str))
        
    # 2. Chia tập dữ liệu (70% Train - 10% Val - 20% Test)
    X_temp, X_test, y_temp, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    X_train, X_val, y_train, y_val = train_test_split(X_temp, y_temp, test_size=0.125, random_state=42)
    
    # 3. Scaling (Chỉ fit trên tập Train)
    scaler = StandardScaler()
    X_train[num_scale] = scaler.fit_transform(X_train[num_scale])
    X_val[num_scale] = scaler.transform(X_val[num_scale])
    X_test[num_scale] = scaler.transform(X_test[num_scale])
    
    # 4. Lưu trữ dữ liệu
    task_path = os.path.join(save_path, task)
    os.makedirs(task_path, exist_ok=True)
    
    datasets = {
        'X_train': X_train, 'X_val': X_val, 'X_test': X_test,
        'y_train': y_train, 'y_val': y_val, 'y_test': y_test
    }
    for name, data in datasets.items():
        data.to_csv(os.path.join(task_path, f'{name}.csv'), index=False)
        
    joblib.dump(scaler, os.path.join(task_path, f'{task}_scaler.pkl'))
    
    print(f"✅ Đã lưu dữ liệu bài toán [{task.upper()}] vào: {task_path}")
    return X_train, X_val, X_test, y_train, y_val, y_test, scaler