# Kịch Bản Phân Tích & Thuyết Trình Sinh Viên: Ý

## Đặt Vấn Đề (Mở Đầu)
"Chào cô/thầy và các bạn. Bài toán chung mà nhóm chúng em đặt ra là **'Tối ưu hóa chiến lược giá và nâng cao tỷ lệ chuyển đổi khách hàng trên sàn thương mại điện tử'**. Để giải quyết bài toán lớn này, phần phân tích EDA của em sẽ tập trung bóc tách 2 mục tiêu SMART cụ thể dựa trên bộ dữ liệu tổng hợp của toàn nhóm (hơn 5.000 sản phẩm)."

---

## 📍 Mục Tiêu 1: Truy tìm "Điểm Ngọt" (Sweet Spot) về Giá theo Ngành Hàng

**1. Dẫn dắt vấn đề:**
"Mục tiêu đầu tiên của em là phân tích xem mức giá nào bán chạy nhất cho từng ngành hàng. Chúng ta không thể áp dụng chung một chiến lược giá cho cả ngành hàng Mẹ & Bé lẫn Điện Thoại. Do đó, em đã phân loại giá sản phẩm thành các phân khúc (Price Buckets) để tìm ra 'Điểm Ngọt' (Sweet Spot)."

**2. Giải thích Biểu Đồ & Dữ liệu (Dựa vào phần 1 trong EDA):**
- **Bảng Top 3 từng ngành hàng:** "Để đáp ứng đúng tiêu chí Đo lường (M), hệ thống đã bóc tách ra bảng danh sách **Top 3 điểm ngọt cho từng ngành hàng**. Bảng này giúp ta biết cụ thể: Ở ngành hàng [Làm Đẹp & Sức Khỏe], 3 phân khúc giá nào mang lại lượt bán tốt nhất; nhưng sang ngành hàng [Điện Thoại], thì 3 mức giá tốt nhất lại hoàn toàn khác."
- **Bảng Pivot & Heatmap:** "Sau đó mọi người có thể nhìn vào Heatmap tổng quan (Biểu đồ nhiệt). Trục tung là các Ngành hàng, trục hoành là các Phân khúc giá. Màu càng đậm thể hiện **Lượt Bán Trung Bình (Nhu cầu mua hàng)** càng kinh khủng."

**3. Đề xuất hành động (Dành cho Quý 3/2026):**
"Từ kết quả này, đề xuất cho quý 3/2026 là: Sàn/Nhà bán hàng cần tập trung nhập hàng và đẩy mạnh ngân sách quảng cáo (Marketing) vào đúng các phân khúc 'Điểm Ngọt' của từng ngành hàng này, tránh lãng phí nguồn lực vào các phân khúc giá rác hoặc khó bán."

---

## 📍 Mục Tiêu 2: Hiệu Quả Của Chính Sách Giảm Giá (Discount)

**1. Dẫn dắt vấn đề:**
"Sang mục tiêu thứ hai, em muốn đánh giá trực diện: Liệu việc giảm giá có thực sự mang lại lượt bán tốt hơn không? Và sự chênh lệch đó lớn đến mức nào?"

**2. Giải thích Biểu Đồ & Dữ liệu (Dựa vào phần 2 trong EDA):**
- **Bảng Thống kê mô tả (Descriptive Statistics):** "Đầu tiên, nhìn vào bảng thống kê, cột 'Trung Bình' cho thấy 1 sản phẩm có dán nhãn giảm giá bán được tới **~484 đơn**, trong khi sản phẩm không giảm giá chỉ le lói ở mức **~55 đơn**. Tức là giảm giá kéo lượt bán trung bình tăng vọt **782%** (gần gấp 9 lần). Đáng sợ hơn là cột 'Trung Vị (50%)': 50% số sản phẩm không giảm giá trên sàn bị ế ẩm (chỉ bán được từ 3 sản phẩm trở xuống), còn nhóm có giảm giá thì 50% số sản phẩm chí ít cũng bán được từ 36 đơn trở lên."
- **Biểu đồ Cột (Bar Chart - So sánh Lượt bán Trung bình):** "Biểu đồ cột này là minh họa trực quan trực tiếp cho con số 484 và 55 đơn bên trên. Sự chênh lệch chiều cao cột cho thấy sức bật phi mã của việc dán nhãn khuyến mãi lên 1 sản phẩm đơn lẻ."
- **Biểu đồ Donut (Tỷ trọng Tổng lượt bán - Nghịch lý thị trường):** "Nhìn sang biểu đồ Donut, chúng ta thấy một nghịch lý thú vị diễn ra. Về 'Số lượng bày bán', hàng Không Giảm Giá chiếm tới 6.709 sản phẩm (đông gấp 4 lần nhóm Có giảm giá). Nhìn có vẻ áp đảo, **NHƯNG** thực tế dòng tiền lại ngược lại! Nhóm thiểu số giảm giá lại **nuốt trọn tới hơn 70% tổng lượt bán** của cả thị trường. Điều này chứng minh khách hàng bị chi phối tâm lý cực mạnh bởi nhãn dán Khuyến Mãi."
- **Biểu đồ Cột nhóm (Grouped Bar Chart - So sánh THEO TỪNG NGÀNH HÀNG):** "Để thỏa mãn triệt để chữ M (đo lường và so sánh theo từng danh mục), biểu đồ cuối cùng phân tách sự khác biệt Có/Không giảm giá trên từng ngành riêng lẻ. Mọi người có thể thấy xu hướng **cột màu xanh (Có giảm giá) luôn cao chót vót** hơn cột màu đỏ (Không giảm giá) một cách đồng đều từ ngành 'Mẹ & Bé', 'Điện Thoại' cho đến 'Sách'."

**3. Đề xuất hành động (Dành cho Quý 3/2026):**
"Mục tiêu này chứng minh tính bắt buộc của các campaign khuyến mãi. Tuy nhiên, thay vì giảm giá đại trà, đề xuất cho quý 3/2026 là thiết lập các mức 'Ngưỡng giảm giá tối thiểu' (Ví dụ: Flash sale giảm ít nhất 5-10%) kết hợp với các phân khúc giá 'Điểm ngọt' ở Mục tiêu 1. Điều này đảm bảo tối ưu tỷ lệ chuyển đổi khách hàng mà không làm thâm hụt biên độ lợi nhuận."

---

## Tổng Kết
"Như vậy, rổ dữ liệu hiện tại cấu trúc rất tốt, thỏa mãn tiêu chí trên 5.000 dòng. Các bước EDA bằng biểu đồ Heatmap, Bar chart và Donut chart đã thành công chỉ ra được đâu là mức giá cần tập trung và khẳng định được trọng số của việc giảm giá. Các insight này hoàn toàn khả thi (A) và liên quan chặt chẽ (R) đến định hướng tối ưu hóa chiến lược giá quý 3/2026. Em xin kết thúc phần phân tích EDA của mình tại đây."