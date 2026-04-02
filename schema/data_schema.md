# Data Schema - Lab 01

> **Bài toán chung**: "Tối ưu hóa chiến lược giá và nâng cao tỷ lệ chuyển đổi khách hàng trên TMĐT"  
> **Nhóm**: 13

---

## 1. Overall Architecture

Data relationship diagram:

dim_platform (top)
    |
    +-- dim_category
    +-- dim_shop  
    +-- fact_product (CENTRAL TABLE)
        |
        +-- fact_review
        +-- bridge_product_promotion
        
Note: No separate orders table needed; use sold_count and shipping in fact_product
- `dim_*`: dữ liệu tham chiếu ít thay đổi (sàn, danh mục, gian hàng)
- `fact_product`: bảng chính, mỗi dòng = 1 sản phẩm tại 1 thời điểm cào
- `fact_review`: chi tiết từng lượt đánh giá
- `bridge_product_promotion`: quan hệ nhiều-nhiều giữa sản phẩm và nhãn KM

---

## 2. `dim_platform` - Sàn thương mại điện tử

| Cột | Kiểu | Nullable | Mô tả |
|-----|------|----------|-------|
| `platform_id` | `str` | No | **PK** - slug viết thường: `shopee`, `lazada`, `tiki`, `sendo` |
| `platform_name` | `str` | No | Tên hiển thị: `Shopee`, `Lazada`, ... |
| `base_url` | `str` | Yes | URL gốc của sàn |

**Ví dụ dữ liệu:**

| platform_id | platform_name | base_url |
|-------------|---------------|----------|
| shopee | Shopee | https://shopee.vn |
| lazada | Lazada | https://lazada.vn |
| tiki | Tiki | https://tiki.vn |

---

## 3. `dim_category` - Danh mục sản phẩm

| Cột | Kiểu | Nullable | Mô tả |
|-----|------|----------|-------|
| `category_id` | `str` | No | **PK** - `{platform_id}_{id_gốc_trên_sàn}` |
| `platform_id` | `str` | No | **FK** - `dim_platform.platform_id` |
| `category_name` | `str` | No | Tên danh mục (tiếng Việt) |
| `parent_category_id` | `str` | Yes | FK tự tham chiếu - danh mục cha (`null` nếu là gốc) |
| `level` | `int` | No | Cấp độ: `1`=gốc, `2`=cấp 2, `3`=cấp 3 |
| `category_url` | `str` | Yes | URL trang danh mục |

**Quy ước `category_id`:** `shopee_1234`, `lazada_5678`

---

## 4. `dim_shop` - Gian hàng / Người bán

| Cột | Kiểu | Nullable | Mô tả |
|-----|------|----------|-------|
| `shop_id` | `str` | No | **PK** - `{platform_id}_{id_gốc}` |
| `platform_id` | `str` | No | **FK** - `dim_platform.platform_id` |
| `shop_name` | `str` | No | Tên gian hàng |
| `shop_type` | `str` | No | `mall` / `official` / `individual` / `unknown` |
| `is_mall` | `bool` | No | `True` nếu là Shopee Mall / LazMall / Tiki Trading |
| `shop_rating` | `float` | Yes | Điểm đánh giá gian hàng (0.0-5.0) |
| `follower_count` | `int` | Yes | Số lượt theo dõi |
| `response_rate` | `float` | Yes | Tỷ lệ phản hồi chat (0.0-1.0) |
| `response_time_hours` | `float` | Yes | Thời gian phản hồi trung bình (giờ) |
| `prep_time_hours` | `float` | Yes | Thời gian chuẩn bị hàng trung bình (giờ) |
| `location` | `str` | Yes | Tỉnh/Thành phố đặt kho |
| `total_products` | `int` | Yes | Số sản phẩm đang bán |
| `shop_url` | `str` | Yes | URL trang gian hàng |
| `crawled_at` | `str` | No | ISO 8601: `2026-03-23T14:00:00+07:00` |

**Quy ước `shop_type`:** viết thường, không dấu cách  
**Quy ước `response_rate`:** lưu dạng thập phân (0.97 thay vì 97%)

---

## 5. `fact_product` - Sản phẩm (Bảng trung tâm)

> Mỗi dòng = 1 sản phẩm tại 1 thời điểm cào.

### 5.1 Khóa & liên kết

| Cột | Kiểu | Nullable | Mô tả |
|-----|------|----------|-------|
| `product_id` | `str` | No | **PK** - `{platform_id}_{id_gốc}` |
| `platform_id` | `str` | No | **FK** - `dim_platform` |
| `category_id` | `str` | No | **FK** - `dim_category` |
| `shop_id` | `str` | No | **FK** - `dim_shop` |

### 5.2 Thông tin cơ bản

| Cột | Kiểu | Nullable | Mô tả |
|-----|------|----------|-------|
| `product_name` | `str` | No | Tên sản phẩm |
| `product_url` | `str` | No | URL trang sản phẩm (dùng truy vết) |

### 5.3 Giá cả

| Cột | Kiểu | Nullable | Mô tả |
|-----|------|----------|-------|
| `price_current` | `float` | No | Giá hiện tại (VND) - đã bỏ ký tự tiền tệ, dấu chấm, dấu phẩy |
| `price_original` | `float` | Yes | Giá gốc trước KM (`null` nếu không có) |
| `discount_percent` | `float` | No | Phần trăm giảm giá (0.0-100.0); `0.0` nếu không giảm |
| `price_ends_with_9` | `bool` | No | `True` nếu đuôi giá kết thúc bằng `9` (ví dụ: 199k, 299k) |
| `price_bucket` | `str` | Yes | Phân khúc giá: `<100k` / `100k-500k` / `500k-1M` / `1M-5M` / `>5M` |

**Cách tính `price_ends_with_9`**: `str(int(price_current))[-1] == '9'`  
**Cách tính `price_bucket`**: điền tự động khi tiền xử lý, không cào trực tiếp

### 5.4 Hiệu suất bán hàng

| Cột | Kiểu | Nullable | Mô tả |
|-----|------|----------|-------|
| `sold_count` | `int` | Yes | Số lượng đã bán (proxy cho tỷ lệ chuyển đổi) |
| `stock` | `int` | Yes | Số lượng còn tồn kho (`-1` nếu không hiển thị) |
| `rating` | `float` | Yes | Điểm đánh giá TB (0.0-5.0) |
| `review_count` | `int` | Yes | Tổng số lượt đánh giá |

### 5.5 Media & Trưng bày

| Cột | Kiểu | Nullable | Mô tả |
|-----|------|----------|-------|
| `image_count` | `int` | Yes | Số ảnh trên trang sản phẩm |
| `has_video` | `bool` | Yes | Có video sản phẩm hay không |
| `review_with_image_count` | `int` | Yes | Số review có kèm ảnh |
| `five_star_with_image_count` | `int` | Yes | Số review 5 sao có kèm ảnh |

### 5.6 Vận chuyển

| Cột | Kiểu | Nullable | Mô tả |
|-----|------|----------|-------|
| `shipping_fee` | `float` | Yes | Phí vận chuyển (VND); `0.0` nếu miễn phí |
| `is_freeship` | `bool` | No | `True` nếu được miễn phí vận chuyển |
| `estimated_delivery_days` | `float` | Yes | Số ngày giao hàng dự kiến |

### 5.7 Khuyến mãi

| Cột | Kiểu | Nullable | Mô tả |
|-----|------|----------|-------|
| `has_freeship_xtra_label` | `bool` | No | Có nhãn "Freeship Xtra" hay không |
| `has_coinback_label` | `bool` | No | Có nhãn "Hoàn xu" / "Shopee Xu" hay không |
| `has_voucher_label` | `bool` | No | Có voucher giảm giá từ shop hay không |
| `promotion_label_count` | `int` | No | Tổng số nhãn KM đang hiển thị |

### 5.8 Metadata

| Cột | Kiểu | Nullable | Mô tả |
|-----|------|----------|-------|
| `crawled_at` | `str` | No | Thời điểm cào - ISO 8601 |
| `crawled_by` | `str` | No | Tên thành viên cào: `thinh` / `tuan` / `y` / `the_anh` / `duong` |

---

## 6. `fact_review` - Chi tiết đánh giá

| Cột | Kiểu | Nullable | Mô tả |
|-----|------|----------|-------|
| `review_id` | `str` | No | **PK** - `{platform_id}_{id_gốc}` |
| `product_id` | `str` | No | **FK** - `fact_product.product_id` |
| `rating` | `int` | No | Số sao (1-5) |
| `has_image` | `bool` | No | Review có kèm ảnh |
| `has_video` | `bool` | No | Review có kèm video |
| `comment_length` | `int` | Yes | Độ dài comment (số ký tự); `0` nếu không có text |
| `review_date` | `str` | Yes | Ngày đánh giá - `YYYY-MM-DD` |
| `crawled_at` | `str` | No | ISO 8601 |

> **Lưu ý**: Bảng này không lưu nội dung comment gốc để tránh file quá nặng.
> Nếu cần NLP/sentiment cập thêm cột `comment_text` (tuỳ chọn).

---

## 7. `bridge_product_promotion` - Nhãn khuyến mãi

| Cột | Kiểu | Nullable | Mô tả |
|-----|------|----------|-------|
| `product_id` | `str` | No | **FK** - `fact_product.product_id` |
| `promotion_label` | `str` | No | Tên nhãn: `freeship_xtra` / `coinback` / `flash_sale` / `voucher` / `mall_sale` |
| `crawled_at` | `str` | No | ISO 8601 |

**PK ghép**: (`product_id`, `promotion_label`)

**Bảng tra cứu nhãn hợp lệ:**

| promotion_label | Hiển thị trên sàn |
|-----------------|-------------------|
| `freeship_xtra` | Freeship Xtra / Freeship Extra |
| `coinback` | Hoàn xu / Shopee Xu / Tích xu |
| `flash_sale` | Flash Sale / Siêu Sale |
| `voucher` | Voucher giảm giá shop |
| `mall_sale` | Mall Sale / LazMall |

---

## 8. Mapping mục tiêu - cột cần cào

| Thành viên | Mục tiêu | Bảng cần | Cột quan trọng |
|------------|----------|----------|----------------|
| **THỊNH 1** | Chất lượng trang - doanh số Mall | `fact_product`, `dim_shop` | `image_count`, `has_video`, `review_with_image_count`, `five_star_with_image_count`, `sold_count`, `is_mall=True` |
| **THỊNH 2** | Vị trí kho & thời gian giao | `dim_shop` | `location`, `prep_time_hours`, `estimated_delivery_days` |
| **TUẤN 1** | Giá x số lượt bán - khoảng giá tối ưu | `fact_product` | `price_current`, `sold_count`, `category_id` |
| **TUẤN 2** | Rating + review_count - mua | `fact_product` | `rating`, `review_count`, `sold_count` |
| **Ý 1** | Price Sweet Spot theo danh mục | `fact_product`, `dim_category` | `price_current`, `price_bucket`, `category_name`, `sold_count` |
| **Ý 2** | Ảnh hưởng của giảm giá đến lượt bán | `fact_product` | `discount_percent`, `sold_count`, `category_id` |
| **THỞA ANH 1** | Uy tín shop - mua | `dim_shop`, `fact_product` | `shop_rating`, `follower_count`, `response_rate`, `sold_count` |
| **THỞA ANH 2** | Phí ship - hoàn tất đơn | `fact_product` | `shipping_fee`, `is_freeship`, `sold_count` |
| **DƯƠNG 1** | Giá kết thúc "9" - lượt bán | `fact_product` | `price_current`, `price_ends_with_9`, `sold_count`, `category_id` |
| **DƯƠNG 2** | Response rate x shop_rating - trust | `dim_shop`, `fact_product` | `response_rate`, `shop_rating`, `sold_count` |

---

## 9. Quy ước định dạng file CSV

| Thuộc tính | Quy định |
|-----------|----------|
| **Encoding** | `utf-8-sig` (UTF-8 with BOM - tương thích Excel) |
| **Delimiter** | dấu phẩy `,` |
| **Null / thiếu** | Để trống (empty cell) - **KHÔNG** dùng `NULL`, `N/A`, `None` |
| **Boolean** | `True` / `False` (viết hoa chữ đầu - chuẩn Python) |
| **Float** | Dấu chấm thập phân `.` - **KHÔNG** dùng dấu phẩy |
| **Tiền tệ** | Số thực (VND), không có ký tự hay dấu phân cách nghìn |
| **Ngày giờ** | ISO 8601: `2026-03-23T14:00:00+07:00` |
| **Header** | Luôn có dòng tiêu đề, đúng tên cột |

---

## 10. Quy ước tên file & thư mục

**Tên file:** `{tên_bảng}_{platform}_{YYYYMMDD}_{crawled_by}.csv`

```
data/raw/
├── fact_product_shopee_20260323_tuan.csv
├── fact_product_shopee_20260323_y.csv
├── fact_review_shopee_20260323_thinh.csv
├── dim_shop_shopee_20260323_the_anh.csv
├── bridge_product_promotion_shopee_20260323_y.csv
└── ...

data/processed/
├── fact_product_merged.csv       ← Ghép tất cả raw, đã tiền xử lý
├── dim_shop_merged.csv
└── ...
```
---

## 11. Checklist trước khi commit dữ liệu

- [ ] File đúng tên: `{bảng}_{platform}_{YYYYMMDD}_{tên_bạn}.csv`
- [ ] Encoding `utf-8-sig`
- [ ] Không thiếu cột bắt buộc (Nullable = No)
- [ ] `price_current` là `float`, không ký tự tiền tệ hay dấu phân cách ngàn
- [ ] `crawled_at` đúng ISO 8601 với timezone `+07:00`
- [ ] `platform_id` viết thường: `shopee`, `lazada`, `tiki`...
- [ ] `product_id` và `shop_id` theo format `{platform_id}_{id_gốc}`
- [ ] `bool` viết là `True`/`False`, không phải `1`/`0` hay `yes`/`no`
- [ ] Đặt vào `data/raw/` - **KHÔNG chỉnh sửa sau khi commit**
- [ ] Tổng `fact_product` toàn nhóm ≥ **5.000 dòng**
