import pandas as pd
import numpy as np
import joblib
import os
import matplotlib.pyplot as plt
import seaborn as sns
from xgboost import XGBRegressor
from lightgbm import LGBMRegressor
from catboost import CatBoostRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

def load_processed_data(task='price', data_path='../data/model/'):
    """Load dữ liệu theo Task (price/sales) từ thư mục tương ứng"""
    path = os.path.join(data_path, task)
    X_train = pd.read_csv(f"{path}/X_train.csv")
    X_val = pd.read_csv(f"{path}/X_val.csv")
    X_test = pd.read_csv(f"{path}/X_test.csv")
    y_train = pd.read_csv(f"{path}/y_train.csv").values.ravel()
    y_val = pd.read_csv(f"{path}/y_val.csv").values.ravel()
    y_test = pd.read_csv(f"{path}/y_test.csv").values.ravel()
    return X_train, X_val, X_test, y_train, y_val, y_test

def get_model(model_name='xgboost'):
    """Khởi tạo mô hình với các tham số tối ưu"""
    if model_name == 'xgboost':
        return XGBRegressor(n_estimators=1000, learning_rate=0.05, max_depth=6, 
                            early_stopping_rounds=50, eval_metric="mae", random_state=42)
    elif model_name == 'lightgbm':
        return LGBMRegressor(n_estimators=1000, learning_rate=0.05, max_depth=6, 
                             random_state=42, verbose=-1)
    elif model_name == 'catboost':
        return CatBoostRegressor(n_estimators=1000, learning_rate=0.05, depth=6, 
                                 random_state=42, verbose=0, early_stopping_rounds=50)

def evaluate_model(model, X_test, y_test):
    """Tính toán metrics trên thang đo thực tế (VND hoặc Sản phẩm)"""
    y_pred_log = model.predict(X_test)
    y_pred = np.expm1(y_pred_log)
    y_true = np.expm1(y_test)
    
    return {
        'MAE': mean_absolute_error(y_true, y_pred),
        'RMSE': np.sqrt(mean_squared_error(y_true, y_pred)),
        'R2': r2_score(y_test, y_pred_log)
    }, y_true, y_pred

def plot_advanced_evaluation(y_true, y_pred, title="Model Evaluation"):
    """Bộ 3 biểu đồ đánh giá chuyên nghiệp"""
    fig, ax = plt.subplots(1, 3, figsize=(20, 6))
    
    # 1. Actual vs Predicted Scatter
    sns.scatterplot(x=y_true, y=y_pred, alpha=0.5, ax=ax[0], color='#0d47a1')
    ax[0].plot([y_true.min(), y_true.max()], [y_true.min(), y_true.max()], 'r--', lw=2)
    ax[0].set_title(f"{title}: Actual vs Predicted")
    ax[0].set_xlabel("Giá trị thực tế")
    ax[0].set_ylabel("Giá trị dự báo")

    # 2. Residual Plot (Kiểm tra sai số hệ thống)
    residuals = y_true - y_pred
    sns.scatterplot(x=y_pred, y=residuals, alpha=0.5, ax=ax[1], color='#1A94FF')
    ax[1].axhline(y=0, color='r', linestyle='--')
    ax[1].set_title(f"{title}: Residual Plot")
    ax[1].set_xlabel("Giá trị dự báo")
    ax[1].set_ylabel("Sai số (Residuals)")

    # 3. Error Distribution (Kiểm tra phân phối sai số)
    sns.histplot(residuals, kde=True, ax=ax[2], color='#002f6c')
    ax[2].set_title(f"{title}: Error Distribution")
    ax[2].set_xlabel("Độ lệch")
    
    plt.tight_layout()
    plt.show()

def plot_feature_importance(model, features):
    """Vẽ Feature Importance chuẩn Seaborn mới"""
    importances = model.feature_importances_
    indices = np.argsort(importances)[::-1]
    feature_names = np.array(features)[indices]
    
    plt.figure(figsize=(10, 6))
    sns.barplot(x=importances[indices], y=feature_names, hue=feature_names, 
                palette="Blues_r", legend=False)
    plt.title("Mức độ quan trọng của các đặc trưng")
    sns.despine()
    plt.show()