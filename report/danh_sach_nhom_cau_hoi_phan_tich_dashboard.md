# Danh sách nhóm câu hỏi phân tích cho Dashboard Lab 01

## Nhóm 1: Toàn cảnh hiệu suất sản phẩm
- Bức tranh tổng quan về số lượng sản phẩm, giá, rating và lượt bán hiện tại là gì?
- Danh mục nào đang có lượt bán trung bình cao nhất?
- Giá sản phẩm đang phân bố như thế nào trên toàn bộ dữ liệu?

Biểu đồ tương ứng:
- KPI tổng quan (Tổng sản phẩm, Giá trung bình, Rating trung bình, Lượt bán trung bình)
- Bar chart top danh mục theo lượt bán trung bình
- Histogram phân bố giá sản phẩm

## Nhóm 2: Chất lượng trưng bày của sản phẩm Mall
- Điểm chất lượng trưng bày có liên hệ như thế nào với lượt bán?
- Số lượng ảnh và việc có video ảnh hưởng thế nào đến hiệu quả bán?
- Top 25% sản phẩm Mall đang có tiêu chuẩn nội dung ra sao?

Biểu đồ tương ứng:
- Scatter điểm trưng bày và lượt bán
- Bar chart lượt bán TB theo nhóm số ảnh
- KPI Mall và khuyến nghị tiêu chuẩn nội dung

## Nhóm 3: Hiệu quả giảm giá theo dải discount
- Nhóm discount nào mang lại lượt bán trung bình cao nhất?
- Phân phối lượt bán thay đổi ra sao giữa các dải giảm giá?
- Nên ưu tiên dải discount nào để thử nghiệm kinh doanh?

Biểu đồ tương ứng:
- Bar chart lượt bán TB theo nhóm discount
- Boxplot phân bố lượt bán theo nhóm discount
- Bảng thống kê band_stats

## Nhóm 4: Độ nhạy giá và doanh thu
- Giá bán thay đổi thì lượt bán thay đổi theo xu hướng nào?
- Khoảng giá nào tối ưu giữa doanh thu và sản lượng bán?
- Có tồn tại quy luật co giãn theo phân khúc giá hay không?

Biểu đồ tương ứng:
- Scatter log-log Price vs Sold Count
- Biểu đồ kết hợp doanh thu và lượt bán theo price bin

## Nhóm 5: Tác động của rating và review
- Rating và review tương quan thế nào với lượt bán?
- Nhóm rating nào có phân phối lượt bán vượt trội?
- Kết hợp rating và review có tạo ra vùng hiệu quả cao không?

Biểu đồ tương ứng:
- Heatmap ma trận tương quan Spearman
- Boxplot phân phối lượt bán theo rating group
- Heatmap hiệu quả Rating x Review

## Nhóm 6: Danh mục và phân khúc giá
- Mỗi danh mục phù hợp nhất với phân khúc giá nào?
- Danh mục nào đang dẫn đầu về lượt bán trung bình theo từng phân khúc?
- Top 3 phân khúc tiềm năng cho từng danh mục là gì?

Biểu đồ tương ứng:
- Grouped bar lượt bán TB theo danh mục và phân khúc giá
- Heatmap danh mục x phân khúc giá
- Bảng top 3 phân khúc theo danh mục

## Nhóm 7: Tác động giảm giá theo danh mục
- Sản phẩm có giảm giá có bán tốt hơn sản phẩm không giảm giá không?
- Tỷ trọng tổng lượt bán đến từ nhóm giảm giá là bao nhiêu?
- Hiệu quả giảm giá có khác nhau giữa các danh mục hay không?

Biểu đồ tương ứng:
- Bar chart lượt bán trung bình có/không giảm giá
- Pie chart tỷ trọng lượt bán theo trạng thái giảm giá
- Grouped bar có/không giảm giá theo danh mục

## Nhóm 8: Phân khúc thị trường theo giá và rating
- Số lượng sản phẩm phân bố như thế nào theo các phân khúc giá?
- Phân khúc nào cân bằng tốt giữa doanh thu và lượt bán?
- Rating trong từng phân khúc giá ảnh hưởng thế nào đến lượt bán?

Biểu đồ tương ứng:
- Bar chart phân bố số lượng sản phẩm theo phân khúc giá
- Boxplot phân phối lượt bán theo phân khúc giá
- Biểu đồ kết hợp doanh thu và sold mean theo phân khúc
- Heatmap price segment x rating
- Scatter giá vs sold theo phân khúc

## Nhóm 9: Hiệu ứng giá tâm lý
- Giá chứa chữ số 9 ở phần đuôi có giúp tăng lượt bán không?
- Mức chênh lệch hiệu quả giữa giá tâm lý và giá thông thường là bao nhiêu?

Biểu đồ tương ứng:
- Bar chart so sánh lượt bán trung bình giữa giá tâm lý và giá thông thường

## Nhóm 10: Trust index và hiệu quả bán hàng
- Trust index có tương quan tích cực với lượt bán hay không?
- Nhóm sản phẩm nào có trust index cao nhưng bán chưa tương xứng?
- Vai trò của Mall trong mối quan hệ trust index và doanh số là gì?

Biểu đồ tương ứng:
- Bubble scatter trust_index_score vs sold_count (size theo revenue_est, màu theo is_mall)

## Nhóm 11: Tổng kết dữ liệu và nguyên tắc trực quan
- Dữ liệu đóng góp theo từng thành viên đang phân bổ ra sao?
- Các nguyên tắc trực quan hoá nào đã được áp dụng nhất quán?

Biểu đồ tương ứng:
- Pie chart tỷ lệ đóng góp dữ liệu theo thành viên
- Checklist nguyên tắc visual đã áp dụng
