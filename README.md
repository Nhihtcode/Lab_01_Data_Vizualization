# LAB 01 — Thu thập & Trực quan hóa Dữ liệu TMĐT

> **Nhóm 13**

---

## 📋 Phân công nhiệm vụ

### 1. Nhiệm vụ thành viên (General Tasks)
- **Cào dữ liệu**: Mỗi người dùng template `crawlers/tiki_crawler.py` (kế thừa `helpers.py`) để thu thập dữ liệu phục vụ cho **2 mục tiêu SMART** riêng.
- **Tiền xử lý**: Tự làm sạch (missing values, outliers), ép kiểu dữ liệu chuẩn và tính các cột derived trong Notebook cá nhân (`notebooks/0x_eda_*.ipynb`).
- **Lưu trữ**: File thô lưu vào `data/raw/` theo đúng quy ước đặt tên để Lead merge.

### 2. Nhiệm vụ Nhóm trưởng (Lead Tasks)
- **Thiết kế Schema**: Duy trì file chuẩn [`schema/data_schema.md`](./schema/data_schema.md).
- **Gom dữ liệu Master**: Sau khi cả nhóm cào xong, Lead sử dụng script `notebooks/00_data_merge.py` để hợp nhất 5 file dữ liệu thô từ `data/raw/` thành 1 file master duy nhất tại `data/processed/`, đảm bảo quy mô ≥ **5.000 dòng**.
- **Dashboard**: Tích hợp các kết quả phân tích của thành viên vào ứng dụng `dashboard/app.py`.

---

## Cấu trúc thư mục

```
LAB_01/
├── schema/
│   └── data_schema.md          ← Schema chuẩn toàn nhóm
├── data/
│   ├── raw/                    ← Dữ liệu thô (KHÔNG chỉnh sửa)
│   └── processed/              ← Dữ liệu đã merge & làm sạch
├── crawlers/
│   ├── utils/
│   │   └── helpers.py          ← Hàm dùng chung (parse_price, get_timestamp...)
│   └── tiki_crawler.py         ← Template crawler (mỗi người copy & chỉnh)
├── notebooks/
│   ├── 00_data_merge.py        ← Merge & validate dữ liệu toàn nhóm
│   ├── 01_eda_thinh.ipynb      ← EDA của THỊNH
│   ├── 02_eda_tuan.ipynb       ← EDA của TUẤN
│   ├── 03_eda_y.ipynb          ← EDA của Ý
│   ├── 04_eda_the_anh.ipynb    ← EDA của THẾ ANH
│   └── 05_eda_duong.ipynb      ← EDA của DƯƠNG
├── dashboard/
│   └── app.py                  ← Streamlit dashboard (3 tab)
├── report/                     ← Báo cáo PDF
├── requirements.txt
└── README.md
```

## Cài đặt môi trường

```bash
pip install -r requirements.txt
```

## Cào dữ liệu

1. Copy `crawlers/tiki_crawler.py`
2. Chỉnh `CRAWLED_BY` (tên bạn) và `CATEGORY_ID` (danh mục bạn cào)
3. Chạy: `python crawlers/tiki_crawler.py`
4. File CSV tự động lưu vào `data/raw/`

### Phân công danh mục (tránh trùng lặp)

| Thành viên | Danh mục | Category ID |
|-----------|---------|------------|
| THỊNH     | Nhà Cửa & Đời Sống | *(điền vào)* |
| TUẤN      | Điện Tử | *(điền vào)* |
| Ý         | Thời Trang Nữ | *(điền vào)* |
| THẾ ANH   | Sức Khỏe & Làm Đẹp | *(điền vào)* |
| DƯƠNG     | Gia Dụng | *(điền vào)* |

## Merge dữ liệu

Sau khi tất cả cào xong, chạy `notebooks/00_data_merge.py` để gộp tất cả file raw thành `data/processed/fact_product_merged.csv`.

## Chạy Dashboard

```bash
streamlit run dashboard/app.py
```

## Quy ước tên file

```
fact_product_{platform}_{YYYYMMDD}_{tên}.csv
```
Ví dụ: `fact_product_tiki_20260325_tuan.csv`

## Quy ước push lên github

Mỗi thành viên sẽ tạo 1 nhánh riêng để push code lên, sau đó nhóm trưởng merge vào nhánh chính
- Đặt tên nhánh theo cú pháp {tên}-data-visualization
- Ví dụ: "tuan-data-visualization"

## Quy ước sửa code trên github
- Chỉ được sửa code trong file của mình. Không sửa phần code của người khác