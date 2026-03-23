# LAB 01 — Thu thập & Trực quan hóa Dữ liệu TMĐT

> **Nhóm 13**

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
│   └── shopee_crawler.py       ← Template crawler (mỗi người copy & chỉnh)
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

1. Copy `crawlers/shopee_crawler.py`
2. Chỉnh `CRAWLED_BY` (tên bạn) và `CATEGORY_ID` (danh mục bạn cào)
3. Chạy: `python crawlers/shopee_crawler.py`
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
Ví dụ: `fact_product_shopee_20260325_tuan.csv`
