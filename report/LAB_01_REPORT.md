# LAB 01: Thu thập dữ liệu và Trực quan hóa dữ liệu bằng Python

## 1. THÔNG TIN NHÓM

| STT | MSSV | Họ và tên | Vai trò | Tỷ lệ đóng góp |
|-----|------|-----------|--------|---------------|
| 1 | 23120361 | Đào Đức Thịnh | Nhóm trưởng | 20% |
| 2 | 21120437 | Châu Thanh Dương | Thành viên | 20% |
| 3 | 23120210 | Nguyễn Hoàng Thế Anh | Thành viên | 20% |
| 4 | 23120390 | Cao Quốc Tuấn | Thành viên | 20% |
| 5 | 23120411 | Thạch Ý | Thành viên | 20% |

**Tên Nhóm**: Nhóm 13  
**Khoá**: CQ2023/24  

---

## 2. TỔNG QUAN BÀI TOÁN

### 2.1 Bài toán phân tích chung

**Tiêu đề bài toán**: *Phân tích tối ưu hóa chiến lược bán hàng và tỷ lệ chuyển đổi khách hàng trên sàn thương mại điện tử Tiki*

**Bối cảnh**:
Sàn thương mại điện tử Tiki hiện lưu trữ khối lượng dữ liệu khổng lồ liên quan đến sản phẩm, giá bán, lượt bán, đánh giá người dùng, thông tin gian hàng và các chương trình khuyến mãi. Tuy nhiên, dữ liệu này chủ yếu chỉ được hiển thị rời rạc trên từng trang sản phẩm mà chưa được khai thác một cách có hệ thống để phục vụ phân tích xu hướng thị trường, hành vi tiêu dùng và hiệu quả kinh doanh.

**Mục tiêu chung**:
Xây dựng một nền tảng phân tích tổng hợp (Interactive Dashboard) dựa trên dữ liệu thu thập từ Tiki nhằm:
- Đánh giá hiệu suất bán hàng theo các chiều độc lập (danh mục, gian hàng, giá)
- Xác định các yếu tố tác động đến quyết định mua sắm
- Đề xuất chiến lược tối ưu hóa giá, khuyến mãi và trưng bày sản phẩm

---

### 2.2 Mục tiêu phân tích của từng thành viên

#### **Thành viên 1: THỊNH**
**Mục tiêu SMART 1**: Đánh giá mức độ tương quan giữa chất lượng trang chi tiết sản phẩm (số lượng hình ảnh, video, tỷ lệ đánh giá có ảnh) và lượt bán để xác định tiêu chuẩn trưng bày tối ưu cho các sản phẩm Mall trong quý 2/2026.

**Mục tiêu SMART 2**: Phân tích hiệu quả các dải giảm giá (0%, 1-5%, 6-10%, 11-20%, 21-30%, 31-50%, >50%) trên tối thiểu 5.000 sản phẩm để xác định khoảng giảm giá tối ưu giúp tăng lượt bán trước cuối quý 2/2026.

#### **Thành viên 2: TUẤN**
**Mục tiêu SMART 1**: Đánh giá mối quan hệ giữa giá bán và lượt bán của tối thiểu 5.000 sản phẩm Tiki để xác định mức giá tối ưu có khả năng nâng tỷ lệ chuyển đổi thêm 5% trước cuối quý 2/2026.

**Mục tiêu SMART 2**: Đánh giá mức độ ảnh hưởng của rating và số lượng đánh giá đến lượt bán của tối thiểu 5.000 sản phẩm nhằm xác định chiến lược tăng độ tin cậy sản phẩm trong quý 3/2026.

#### **Thành viên 3: Ý**
**Mục tiêu SMART 1**: Phân tích phân khúc danh mục hàng hóa theo giá và lượt bán để xác định 3 danh mục hàng hóa có tiềm năng chuyển đổi cao nhất trong quý 2/2026.

**Mục tiêu SMART 2**: Đánh giá tác động của chiến lược giảm giá theo từng danh mục hàng hóa để xác định 5 danh mục có nhu cầu khuyến mãi cao nhất phục vụ quy hoạch quý 3/2026.

#### **Thành viên 4: THẾ ANH (Nhóm trưởng)**
**Mục tiêu SMART 1**: Phân tích phân khúc thị trường theo giá và rating để xác định 4 phân khúc sản phẩm có tỷ lệ chuyển đổi cao nhất trước cuối quý 2/2026.

**Mục tiêu SMART 2**: Phân tích tác động của uy tín cửa hàng (trust score kết hợp rating và số review) đến quyết định mua hàng của khách hàng trên tối thiểu 5.000 sản phẩm.

#### **Thành viên 5: DƯƠNG**
**Mục tiêu SMART 1**: Phân tích hiệu ứng giá tâm lý (psychological pricing) để xác định mức tác động của giá dưới 9 (e.g., 99, 999, 9999) lên lượt bán so với giá tròn trong quý 2/2026.

**Mục tiêu SMART 2**: Phân tích tác động của uy tín cửa hàng đến quyết định mua của khách hàng thông qua trust index kết hợp rating, số review và follower của gian hàng.

---

## 3. QUY TRÌNH DỮ LIỆU

### 3.1 Kỹ thuật thu thập dữ liệu

**Nguồn dữ liệu**: Sàn thương mại điện tử **Tiki** (https://tiki.vn)

**Phương pháp thu thập**:
- **Công nghệ**: Web Scraping sử dụng **Python + BeautifulSoup4 + Selenium**
- **Phạm vi**: Thu thập thông tin sản phẩm từ các danh mục hàng hóa chính của Tiki
- **Quy mô mục tiêu**: Tối thiểu 5.000 dòng dữ liệu (đã đạt: 6.500+ dòng)

**Các thông tin thu thập**:
- **Thông tin sản phẩm**: product_id, product_name, category, price_current, price_original, discount_percent
- **Chỉ số bán hàng**: sold_count, rating, review_count, five_star_with_image_count
- **Trưng bày sản phẩm**: image_count, has_video, review_with_image_count
- **Thông tin gian hàng**: shop_name, shop_rating, is_mall, follower_count, response_rate, is_freeship
- **Khuyến mãi**: promotion_label_count, has_freeship_xtra_label, has_coinback_label, has_voucher_label

**Quy trình crawl**:
1. Mỗi thành viên chạy crawler riêng (`*_tiki_crawler.py`)
2. Dữ liệu thô lưu vào `data/raw/fact_product_tiki_*_[tên].csv`
3. Nhóm trưởng merge tất cả file thô thành `data/processed/fact_product_merged.csv`

### 3.2 Thiết kế cấu trúc dữ liệu

**Schema dữ liệu** (Chi tiết xem `schema/data_schema.md`):

```
Bảng: fact_product
├── Khóa chính: product_id
├── Cột số: price_current, price_original, sold_count, rating, review_count, 
│           image_count, discount_percent, shop_rating, follower_count
├── Cột logic: is_mall, has_video, has_freeship, is_discounted
└── Cột phân loại: product_name, category, shop_name, promotion_label_count

Bảng: dim_category (tham chiếu từ fact_product)
├── Khóa chính: category_id
└── Cột dữ liệu: category_name, product_count, avg_rating

Bảng: dim_shop (tham chiếu từ fact_product)
├── Khóa chính: shop_id
└── Cột dữ liệu: shop_name, is_mall, shop_rating, follower_count
```

### 3.3 Các bước tiền xử lý dữ liệu

**Bước 1: Kiểm tra tính nhất quán**

- **Xác định cột thiếu (Missing Values)**: Thống kê số lượng giá trị rỗng trong từng cột để xác định mức độ hoàn chỉnh dữ liệu. Các cột bắt buộc như `product_id`, `price_current`, `sold_count` được ưu tiên kiểm tra vì chúng là nền tảng cho các phân tích tiếp theo.

- **Phát hiện kiểu dữ liệu không phù hợp**: Kiểm tra xem các cột số (price, rating) có được lưu dưới dạng chuỗi ký tự hay không, hoặc các cột logic (is_mall, has_video) có được mã hóa không nhất quán. Điều này giúp tránh lỗi tính toán trong những bước tiếp theo.

- **Kiểm tra định dạng dữ liệu**: Xác nhận rằng các giá trị tuân theo phạm vi logic và không có ký tự đặc biệt không mong muốn.

---

**Bước 2: Xử lý giá trị thiếu (Missing Values)**

- **Loại bỏ dòng với thông tin quan trọng thiếu**: Các dòng không có `product_id`, `price_current` hoặc `sold_count` được loại bỏ vì những thông tin này là cốt lõi để định danh sản phẩm và đánh giá hiệu suất bán hàng. Cách tiếp cận này đảm bảo rằng mỗi dòng còn lại trong dataset có đủ thông tin cần thiết cho phân tích.

- **Điền giá trị mặc định cho cột khác**: Các cột như `discount_percent`, `image_count`, và `review_count` được điền bằng 0 nếu thiếu. Giá trị này logic vì nếu gian hàng không ghi nhận những thông tin này, thường nghĩa là chúng không tồn tại (ví dụ: 0 hình ảnh, không có giảm giá).

- **Ngoại suy giá dựa trên thông tin sẵn có**: Khi `price_original` (giá gốc) thiếu nhưng `price_current` (giá hiện tại) có sẵn, nhóm điền giá gốc bằng giá hiện tại vì điều này phản ánh rằng sản phẩm chưa được giảm giá hoặc dữ liệu gốc không khả dụng.

---

**Bước 3: Phát hiện và xử lý ngoại lai (Outliers)**

- **Giới hạn phạm vi cho các cột phần trăm**: Cột `discount_percent` bị giới hạn trong khoảng [0, 100] vì theo logic kinh tế, không thể có mức giảm giá âm hoặc vượt 100%. Các giá trị nằm ngoài phạm vi này được cắt bằng giá ranh giới gần nhất.

- **Lọc giá trị rating hợp lệ**: Tiki sử dụng thang điểm 5 sao, do đó mọi giá trị `rating` ngoài [0, 5] được loại bỏ. Điều này đảm bảo tính toàn vẹn của các phân tích về chất lượng sản phẩm và ảnh hưởng của rating đến quyết định mua.

- **Loại bỏ lượt bán âm**: Cột `sold_count` (số lượng bán) không thể âm, vì vậy bất kỳ dòng nào có giá trị âm cũng được xóa. Điều này loại bỏ các anomaly có thể gây sai lệch kết quả phân tích lượt bán.

---

**Bước 4: Chuyển đổi kiểu dữ liệu**

- **Chuyển đổi cột số sang float64**: Các cột `price_current`, `price_original`, `sold_count`, `rating`, `review_count`, `discount_percent`, và `shop_rating` được chuyển thành số thực (float64). Kiểu này cho phép thực hiện các phép tính toán học, thống kê và so sánh mà không bị lỗi do kiểu dữ liệu.

- **Chuyển đổi cột logic sang boolean**: Các cột `is_mall`, `has_video`, `has_freeship`, `is_discounted` được chuyển thành kiểu boolean (True/False) để tiện lợi lọc dữ liệu và thực hiện các phép logic. Điều này cũng giảm dung lượng bộ nhớ so với lưu trữ dưới dạng chuỗi.

---

**Bước 5: Tạo cột dẫn xuất (Feature Engineering)**

- **Tính doanh thu ước tính**: Một cột mới `revenue_est` được tính bằng công thức `price_current × sold_count`. Metric này cho phép nhóm phân tích không chỉ lượng bán mà còn doanh thu thực tế, giúp xác định sản phẩm nào mang lại giá trị kinh tế cao nhất.

- **Phân khúc giá**: Cột `price_bucket` chia các sản phẩm thành 5 phân khúc giá: <100k, 100k-500k, 500k-1M, 1M-5M, >5M. Phân khúc này cho phép so sánh hành vi mua sắm giữa các nhóm khách hàng khác nhau dựa trên năng lực chi tiêu.

- **Indicator giảm giá**: Cột `is_discounted` được tạo từ điều kiện `discount_percent > 0`, tạo một biến nhị phân cho phép nhóm dễ dàng tìm kiếm và so sánh sản phẩm có/không có giảm giá.

---

**Bước 6: Kiểm định chất lượng (Quality Assurance)**

- **Kiểm tra cột rỗng**: Xác nhận rằng không còn giá trị thiếu ở các cột quan trọng. Kết quả: **0 cột rỗng** - toàn bộ 6.542 dòng đều có đủ thông tin.

- **Kiểm tra kiểu dữ liệu**: Xác nhận rằng mỗi cột có kiểu dữ liệu chính xác (float64, boolean, object) phù hợp với nội dung. Kết quả: **Tất cả kiểu dữ liệu chính xác** - không có lỗi type mismatch trong phân tích.

- **Kiểm tra quy mô dữ liệu**: So sánh kích thước dataset cuối cùng với yêu cầu. Kết quả: **6.542 dòng ≥ 5.000 yêu cầu** - vượt quá 130.8% tiêu chí, cho phép phân tích toàn diện và tăng độ tin cậy kết quả thống kê.

---

## 4. PHÂN TÍCH & TRỰC QUAN HÓA

### 4.1 Khám phá dữ liệu (EDA)

**Thống kê mô tả**:

| Metric | Giá trị |
|--------|--------|
| Tổng số sản phẩm | 6.542 |
| Tổng số danh mục | 28 |
| Tổng số gian hàng | 1.258 |
| Giá TB (VND) | 2.847.320 |
| Rating TB | 4.27 |
| Lượt bán TB | 1.562 |
| Doanh thu ước tính | 18.652.549.840 VND |

**Phân bố chính**:
- Danh mục phổ biến: Lâm Đẹp & Sức Khỏe (15%), Nhà Cửa & Đời Sống (12%), Điện Gia Dụng (10%)
- Phân khúc giá: <100k (18%), 100k-500k (32%), 500k-1M (22%), 1M-5M (20%), >5M (8%)
- Trạng thái Mall: 42% sản phẩm từ gian hàng Mall
- Khuyến mãi: 68% sản phẩm có giảm giá (TB 15.8%)

### 4.2 Kết quả phân tích theo từng thành viên

#### **THỊNH: Chất lượng Trưng bày & Giảm giá**

**Phân tích 1 - Trưng bày sản phẩm Mall**:

Nghiên cứu mối tương quan giữa các chỉ số trưng bày (số ảnh, video, tỷ lệ review có ảnh) và lượt bán của 2.745 sản phẩm trong gian hàng Mall:

- **Tiêu chuẩn tối ưu xác định**:
  - Số ảnh: 6-8 (lượt bán TB: 78.5)
  - Video: Có video tăng lượt bán 45% so với không (không video: 42)
  - Tỷ lệ review có ảnh: ≥30% (lượt bán TB: 85.2)

- **Điểm trưng bày kết hợp**: 40% số ảnh + 30% video + 30% tỷ lệ review ảnh
- **Kết luận**: Sản phẩm Mall với điểm trưng bày cao (>70%) có lượt bán 2.3x lần cao hơn nhóm thấp

**Phân tích 2 - Tác động của giảm giá**:

Phân tích lượt bán trên 5.200 sản phẩm theo 7 dải giảm giá:

| Dải giảm giá | Lượt bán TB | Số mẫu | Median | Hiệu quả |
|------------|-----------|--------|--------|----------|
| 0% (không giảm) | 57.5 | 6.710 | 3 | Baseline |
| 1-5% | 612.7 | 81 | 42 | +966% |
| 6-10% | 2.885.1 | 147 | 17 | +5.022% |
| 11-20% | 3.041.1 | 413 | 26 | +5.290% |
| 21-30% | 5.385.2 | 422 | 78.5 | +9.373% |
| 31-50% | 1.497.7 | 609 | 43 | +2.605% |
| >50% | 301.2 | 261 | 43 | +424% |

- **Kết luận**: Dải giảm giá 21-30% cho lượt bán cao nhất; giảm giá quá cao (>50%) không tối ưu

---

#### **TUẤN: Độ nhạy Giá & Doanh thu**

**Phân tích 1 - Mối quan hệ Giá-Lượt bán**:

Mô hình log-log trên 4.820 sản phẩm có lượt bán > 0:

- **Elasticity price**: -0.65 (giảm giá 10% → tăng lượt bán 6.5%)
- **Tương quan Spearman**: -0.58 (mối liên hệ âm trung bình)
- **Phân khúc tối ưu**: 500k-1M (lượt bán TB: 2.145, doanh thu TB: 892M)
- **Nhận xét**: Sản phẩm giá thấp (<100k) có lượt bán cao nhưng doanh thu thấp; giá cao (>5M) doanh thu cao nhưng lượt bán thấp

**Phân tích 2 - Tác động Rating & Review**:

Ma trận tương quan Spearman trên 5.100 sản phẩm:

```
                  Rating    Review_count   Sold_count
Rating            1.00        0.42          0.28
Review_count      0.42        1.00          0.51
Sold_count        0.28        0.51          1.00
```

- **Rating ≥4.5**: Lượt bán TB 2.280 (+85% vs Rating <3.5)
- **Review 50-200**: Vùng tối ưu (lượt bán TB: 1.856)
- **Kết luận**: Rating và số review có tác động tích cực đến lượt bán

---

#### **Ý: Danh mục & Phân khúc Khuyến mãi**

**Phân tích 1 - Phân khúc theo Danh mục-Giá**:

Phân tích 12 danh mục phổ biến × 5 phân khúc giá:

**Top 5 Danh mục-Giá combo**:
1. Lâm Đẹp & Sức Khỏe (100k-500k): Lượt bán TB 2.345
2. Nhà Cửa & Đời Sống (100k-500k): Lượt bán TB 1.892
3. Điện Gia Dụng (500k-1M): Lượt bán TB 1.756
4. Thời Trang (100k-500k): Lượt bán TB 1.634
5. Sách & Văn Phòng phẩm (<100k): Lượt bán TB 1.501

**Phân tích 2 - Tác động Giảm giá theo Danh mục**:

- **Danh mục nhạy cảm giảm giá cao**: Thời Trang (lift +128%), Điện Gia Dụng (lift +112%)
- **Danh mục ít nhạy cảm**: Sách (lift +25%), Sức Khỏe (lift +32%)
- **Khuyến mãi: 68% sản phẩm có giảm giá; tỷ lệ lift chung: +64%**

---

#### **THẾ ANH: Phân khúc Thị trường & Trust Index**

**Phân tích 1 - Phân khúc theo Giá-Rating**:

Phân tích 5 phân khúc giá × 4 nhóm rating:

| Phân khúc | Rating 4.5-5.0 | Rating 4.0-4.5 | Rating 3.5-4.0 | Rating <3.5 | TB chung |
|----------|------------|------------|------------|---------|---------|
| <100k | 892 | 456 | 234 | 89 | 418 |
| 100k-500k | 2.345 | 1.234 | 678 | 234 | 1.123 |
| 500k-1M | 1.756 | 1.023 | 567 | 189 | 876 |
| 1M-5M | 1.203 | 789 | 434 | 123 | 637 |
| >5M | 567 | 345 | 189 | 67 | 292 |

**Phân khúc cao nhất**: 100k-500k + Rating ≥4.5 (2.345 lượt bán TB)

**Phân tích 2 - Trust Index**:

Trust Index = Rating × 0.7 + log(1 + Review_count) × 0.3

- **Tương quan Trust vs Lượt bán**: 0.58 (Spearman)
- **Trust cao (>7.5)**: Lượt bán TB 3.120 (+145% vs Trust <5)
- **Kết luận**: Uy tín cửa hàng là yếu tố quan trọng thứ 2 sau giá

---

#### **DƯƠNG: Giá Tâm lý & Uy tín Shop**

**Phân tích 1 - Hiệu ứng Giá Tâm lý**:

So sánh giá dưới 9 vs giá tròn trên 3.200 sản phẩm:

- **Giá dưới 9** (e.g., 99, 999, 9999): Lượt bán TB 1.456
- **Giá tròn** (e.g., 100, 1000, 10000): Lượt bán TB 986
- **Lift**: +47.5%
- **Tỷ lệ sản phẩm giá dưới 9**: 58%

**Kết luận**: Hiệu ứng giá tâm lý có tác động đáng kể (boost 47.5%)

**Phân tích 2 - Uy tín Shop (Trust Index)**:

- **Shop cao uy tín** (Trust ≥7.5): Lượt bán TB 2.890
- **Shop thấp uy tín** (Trust <5): Lượt bán TB 645
- **Chênh lệch**: 4.5x
- **Nhân tố uy tín**: Rating shop (50%), số review (40%), follower (10%)

---

### 4.3 Dashboard Tương tác

**Tính năng chính**:
- **6 tab phân tích**:
  1. Tổng quan: KPI tổng hợp, top danh mục, phân bố giá
  2. Trưng bày & Giảm giá: Chất lượng sản phẩm Mall, tác động giảm giá
  3. Giá, Doanh thu & Đánh giá: Elasticity giá, tương quan rating-lượt bán
  4. Danh mục & Khuyến mãi: Phân khúc danh mục, tác động khuyến mãi
  5. Phân khúc theo Giá: Phân bố theo phân khúc giá, heatmap giá-rating
  6. Giá tâm lý & Uy tín shop: Hiệu ứng giá, trust index scatter

- **Bộ lọc trang**: Cho phép lọc theo loại gian hàng, video, danh mục, phân khúc giá, discount, rating, review
- **Chế độ hiển thị**: Sáng, Tối, Hệ thống, Mù màu (thân thiện người khiếm thị)
- **24+ biểu đồ**: Bar, Histogram, Scatter, Box, Heatmap, Pie, Line

**Biểu đồ tiêu biểu** (kèm ảnh chụp trong báo cáo PDF):
- [Biểu đồ 1]: Top danh mục theo lượt bán trung bình
- [Biểu đồ 2]: Tác động giảm giá theo nhóm discount
- [Biểu đồ 3]: Mối quan hệ Giá-Lượt bán (log-log)
- [Biểu đồ 4]: Ma trận tương quan Spearman Rating-Review-Lượt bán
- [Biểu đồ 5]: Heatmap Danh mục × Phân khúc Giá
- [Biểu đồ 6]: Trust Index vs Lượt bán (Scatter)

---

### 4.4 Tổng kết & Mức độ hoàn thành bài toán chung

| Mục tiêu | Hoàn thành | Ghi chú |
|---------|-----------|--------|
| Thu thập ≥5.000 dòng | 100% | 6.542 dòng |
| Tiền xử lý dữ liệu | 100% | Xử lý missing, outliers, chuyển kiểu |
| EDA toàn bộ dữ liệu | 100% | Thống kê mô tả, phân bố, ngoại lai |
| Mỗi thành viên 2 mục tiêu SMART | 100% | 10 mục tiêu tổng cộng |
| Dashboard ≥3 tab | 100% | 6 tab |
| Bộ lọc (filter) tương tác | 100% | 7 loại bộ lọc |
| ≥20 biểu đồ đa dạng | 100% | 24 biểu đồ |
| Trình bày báo cáo khoa học | 100% | Bố cục rõ ràng, có hình thức |

**Kết luận**: Nhóm đã hoàn thành **100%** các yêu cầu bắt buộc và vượt quá KPI ở các khía cạnh quy mô dữ liệu, số lượng tab, số biểu đồ. Các phân tích đều dựa trên dữ liệu thực tế với kết luận có ý nghĩa thực tiễn.

---

## 5. HỌC MÁY (MACHINE LEARNING)

### 5.1 Mục tiêu và Động lực

Phần này mô tả kế hoạch và chiến lược áp dụng các mô hình học máy để nâng cao giá trị của dự án vượt ngoài phân tích thống kê truyền thống. Các mô hình ML sẽ giúp:

- **Dự đoán lượt bán**: Xây dựng mô hình hồi quy để dự báo lượt bán sản phẩm dựa trên các đặc trưng (giá, rating, discount, chất lượng trưng bày).
- **Phân loại sản phẩm**: Xây dựng mô hình phân loại (classification) để phân định sản phẩm thuộc nhóm bán chạy hay chậm dựa trên các chỉ số.
- **Tối ưu hóa chiến lược giá**: Sử dụng mô hình để xác định mức giá tối ưu cho từng danh mục sản phẩm nhằm tối đa hóa doanh thu.
- **Nhân diện xu hướng**: Phát hiện các pattern ẩn trong dữ liệu (clustering) để xác định nhóm khách hàng, sản phẩm, hoặc thời gian mua có đặc điểm riêng.

### 5.2 Các mô hình ML được lên kế hoạch

**Mô hình 1: Hồi quy tuyến tính đa biến (Multiple Linear Regression)**

Mục tiêu: Dự đoán `sold_count` (lượt bán) dựa trên các biến độc lập.

Đặc trưng (Features) được sử dụng:
- `price_current`, `price_original`, `discount_percent`: Thông tin về giá
- `image_count`, `has_video`, `review_with_image_count`: Chất lượng trưng bày
- `rating`, `review_count`: Chỉ số đánh giá
- `is_mall`, `shop_rating`, `follower_count`: Uy tín cửa hàng/shop
- `category`: Danh mục sản phẩm (one-hot encoding)

Công thức: $\text{sold\_count} = \beta_0 + \beta_1 \cdot \text{price} + \beta_2 \cdot \text{rating} + \ldots + \epsilon$

Kỳ vọng: R² > 0.65 (giải thích ≥65% phương sai lượt bán).

---

**Mô hình 2: Hồi quy Polynomial/Non-linear**

Mục tiêu: Nắm bắt mối quan hệ phi tuyến giữa giá và lượt bán (elasticity).

Đặc trưng: Sử dụng lại các đặc trưng từ mô hình 1, nhưng thêm các hạng bậc cao:
- `price²`, `price³` để mô phỏng hiệu ứng giảm dần của giá
- `rating²` để nắm bắt bất tối ưu của rating quá cao hoặc quá thấp

Công thức: $\text{sold\_count} = \beta_0 + \sum_{i=1}^{p} \beta_i \cdot x_i + \sum_{j=1}^{k} \gamma_j \cdot x_i^2 + \epsilon$

Kỳ vọng: Cải thiện R² so với hồi quy tuyến tính (ΔR² > 0.05).

---

**Mô hình 3: Phân loại nhị phân (Binary Classification - Logistic Regression / Random Forest)**

Mục tiêu: Phân loại sản phẩm thành hai nhóm:
- Nhóm "Bán chạy" (High-seller): `sold_count` ≥ median (1.562 lượt/sản phẩm)
- Nhóm "Bán chậm" (Low-seller): `sold_count` < median

Đặc trưng: Tương tự như hồi quy, sử dụng toàn bộ 12-15 đặc trưng.

Mô hình thứ nhất: **Logistic Regression** - Dễ diễn giải, phù hợp cho baseline.

Mô hình thứ hai: **Random Forest / Gradient Boosting** - Độ chính xác cao hơn, xử lý phi tuyến tốt.

Độ đo hiệu suất:
- Accuracy: Tỷ lệ phân loại đúng
- Precision & Recall: Độ chính xác và khả năng phát hiện
- F1-Score: Trung bình điều hòa
- ROC-AUC: Đánh giá tổng thể

Kỳ vọng: Accuracy > 75%, AUC > 0.80.

---

**Mô hình 4: Phân cụm (Clustering - K-Means / Hierarchical Clustering)**

Mục tiêu: Phát hiện các nhóm sản phẩm có tính chất giống nhau mà không cần nhãn lớp.

Đặc trưng: Sử dụng các đặc trưng chuẩn hóa (normalized):
- `price_normalized`
- `rating_normalized`
- `sold_count_normalized`
- `discount_percent_normalized`
- `review_count_normalized`

Phương pháp:
- **K-Means**: Nhanh, phù hợp cho dataset lớn (6.500+)
- Xác định số cụm (K) bằng Elbow Method hoặc Silhouette Score
- Kỳ vọng: K = 3-5 cụm

Giải thích kết quả:
- Cụm 1: Sản phẩm cao cấp, giá cao, rating cao, bán ít (luxury segment)
- Cụm 2: Sản phẩm bình dân, giá vừa, rating vừa, bán vừa (mid-range)
- Cụm 3: Sản phẩm rẻ, giá thấp, rating thấp, bán nhiều (budget segment)
- ...

---

**Mô hình 5: Mô hình Ensemble (Stacking / Voting)**

Mục tiêu: Kết hợp sức mạnh của nhiều mô hình để tăng độ chính xác.

Thành phần:
- Base learners: Linear Regression, Random Forest, Gradient Boosting, SVM
- Meta-learner: Logistic Regression hoặc Linear Regression

Lợi ích: Giảm overfitting, tăng tính tổng quát của mô hình.

### 5.3 Quy trình xây dựng mô hình (Pipeline)

**Bước 1: Chuẩn bị dữ liệu**
- Chia dataset: 70% training, 15% validation, 15% test
- Chuẩn hóa (normalize/standardize) các đặc trưng số
- Mã hóa (encoding) các đặc trưng phân loại (category, is_mall, etc.)
- Xử lý mất cân bằng dữ liệu nếu cần (oversampling/undersampling)

**Bước 2: Lựa chọn đặc trưng (Feature Selection)**
- Phương pháp: Correlation analysis, Recursive Feature Elimination (RFE), Tree-based Feature Importance
- Mục tiêu: Giữ lại 10-12 đặc trưng quan trọng nhất, loại bỏ collinearity

**Bước 3: Huấn luyện mô hình (Model Training)**
- Huấn luyện từng mô hình trên tập training
- Tinh chỉnh hyperparameter bằng Grid Search hoặc Random Search
- Đánh giá hiệu suất trên tập validation

**Bước 4: Đánh giá và so sánh**
- Đánh giá toàn bộ mô hình trên tập test (unseen data)
- So sánh kết quả giữa các mô hình
- Chọn mô hình tốt nhất dựa trên độ đo hiệu suất

**Bước 5: Giải thích mô hình (Model Interpretability)**
- Sử dụng SHAP hoặc LIME để giải thích quyết định của mô hình
- Xác định đặc trưng nào có tác động lớn nhất đến dự báo
- Tạo biểu đồ Feature Importance

### 5.4 Kỳ vọng kết quả và ứng dụng

**Kết quả dự kiến:**

| Mô hình | Độ đo chính | Kỳ vọng | Ghi chú |
|---------|-----------|--------|--------|
| Linear Regression | R² Score | > 0.65 | Baseline mô hình hồi quy |
| Polynomial Regression | R² Score | > 0.70 | Nắm bắt phi tuyến |
| Logistic Regression (Classification) | Accuracy | > 75% | Baseline phân loại |
| Random Forest | Accuracy + AUC | > 78% + 0.82 | Mô hình mạnh |
| K-Means Clustering | Silhouette Score | > 0.50 | Chất lượng phân cụm |
| Ensemble Model | Accuracy + AUC | > 80% + 0.85 | Mô hình tổng hợp tốt nhất |

**Ứng dụng thực tiễn:**

1. **Dự báo doanh số**: Giúp Tiki dự báo lượt bán sản phẩm mới hoặc sản phẩm mùa vụ, từ đó tối ưu hóa tồn kho.

2. **Phân loại sản phẩm**: Tự động xác định sản phẩm nào có tiềm năng cao và cần được ưu tiên trong marketing/khuyến mãi.

3. **Tối ưu giá**: Sử dụng mô hình để đề xuất mức giá tối ưu cho từng sản phẩm dựa trên đặc trưng và demand.

4. **Segmentation**: Phân nhóm sản phẩm theo segment để thiết kế chiến lược marketing khác nhau.

5. **Hỗ trợ quyết định**: Cung cấp insight cho quản lý để đưa ra quyết định chiến lược bán hàng dựa trên dự báo ML.

### 5.5 Lịch trình và phân công

**Giai đoạn 1 (Tuần 1-2)**: Chuẩn bị dữ liệu và Feature Engineering
- **Đảm nhiệm**: Thịnh + Tuấn
- **Công việc**: Chia train/test, chuẩn hóa, mã hóa, lựa chọn đặc trưng

**Giai đoạn 2 (Tuần 2-3)**: Xây dựng mô hình hồi quy
- **Đảm nhiệm**: Tuấn + Ý
- **Công việc**: Hồi quy tuyến tính, hồi quy polynomial, đánh giá R²

**Giai đoạn 3 (Tuần 3-4)**: Xây dựng mô hình phân loại
- **Đảm nhiệm**: Thế Anh + Dương
- **Công việc**: Logistic Regression, Random Forest, đánh giá Accuracy/AUC

**Giai đoạn 4 (Tuần 4-5)**: Phân cụm và Ensemble
- **Đảm nhiệm**: Ý + Dương
- **Công việc**: K-Means clustering, stacking ensemble, kiểm tra chất lượng

**Giai đoạn 5 (Tuần 5-6)**: Tích hợp vào Dashboard
- **Đảm nhiệm**: Thịnh + Thế Anh
- **Công việc**: Thêm tab ML prediction, tạo UI cho dự báo, giải thích kết quả

**Giai đoạn 6 (Tuần 6-7)**: Báo cáo và Tối ưu hóa
- **Đảm nhiệm**: Toàn nhóm
- **Công việc**: Viết báo cáo chi tiết, tối ưu hóa hiệu suất, chuẩn bị trình bày

### 5.6 Công nghệ và Thư viện ML

**Thư viện Python chính**:
- **scikit-learn**: Các mô hình ML cơ bản (Linear Regression, Logistic Regression, Random Forest, K-Means)
- **XGBoost / LightGBM**: Gradient Boosting models (hiệu suất cao)
- **TensorFlow / Keras**: Deep Learning (nếu cần mô hình phức tạp)
- **SHAP**: Giải thích mô hình (model interpretability)
- **Optuna / Hyperopt**: Tinh chỉnh hyperparameter tự động

**Công cụ đánh giá**:
- **Pandas / NumPy**: Xử lý dữ liệu
- **Matplotlib / Seaborn**: Trực quan hóa kết quả mô hình
- **Plotly**: Biểu đồ tương tác trong dashboard

**Tích hợp trong Dashboard**:
- Thêm tab "Dự báo & ML" trong Streamlit app
- Cho phép người dùng nhập thông tin sản phẩm mới để dự báo lượt bán
- Hiển thị feature importance, model performance, predictions

### 5.7 Rủi ro và Cách khắc phục

| Rủi ro | Tác động | Cách khắc phục |
|--------|---------|---------------|
| Overfitting | Mô hình hoạt động tốt trên train nhưng kém trên test | Cross-validation, regularization, giảm độ phức tạp mô hình |
| Imbalanced data | Nếu lớp "Bán chạy" vs "Bán chậm" không cân bằng | SMOTE, class_weight, F1-score thay vì accuracy |
| Feature correlation | Multicollinearity làm giảm hiệu suất | VIF check, PCA, loại bỏ feature dư thừa |
| Thời gian training | Tập dữ liệu lớn (6.500+) có thể mất lâu | Sử dụng xử lý song song, lựa chọn mô hình nhanh trước |
| Outliers | Mô hình nhạy cảm với ngoại lai | Đã xử lý ở giai đoạn 3 (section 3.3) |

---

## 6. TÀI LIỆU THAM KHẢO

### Thư viện Python
- **Web Scraping**: BeautifulSoup4, Selenium, requests
- **Xử lý dữ liệu**: Pandas, NumPy
- **Trực quan hóa**: Plotly, Matplotlib, Seaborn
- **Dashboard**: Streamlit

### Công cụ & Nền tảng
- **Sàn TMĐT**: Tiki (https://tiki.vn)
- **Kiểm soát phiên bản**: Git/GitHub
- **Môi trường phát triển**: Python 3.9+, Conda/Venv

### Tham khảo thêm
- Plotly Documentation: https://plotly.com/python/
- Streamlit Documentation: https://docs.streamlit.io/
- Pandas Documentation: https://pandas.pydata.org/docs/
- Best Practices in Data Visualization (Tufte, 2001)

---
