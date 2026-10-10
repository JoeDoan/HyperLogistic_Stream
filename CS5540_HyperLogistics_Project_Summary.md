# CS 5540 Principles of Big Data Management
## Tổng Quan Dự Án, Mục Tiêu, Yêu Cầu & Hướng Dẫn Chọn Dataset

**Môn học:** COMP-SCI 5540 Principles of Big Data Management (Fall 2026, UMKC)  
**Giảng viên:** Dr. Md Yusuf Sarwar Uddin  
**Nhóm thực hiện:** Joe Doan & Tony Nguyen  
**Tên dự án lựa chọn:** HyperLogistics-Stream (Real-Time Hub-to-Hub ETA Prediction with Kafka and Spark Structured Streaming)  

---

### 1. Tóm Tắt Dự Án (Project Summary)

* **Bối cảnh & Vấn đề:** Trong vận tải chặng giữa (*middle-mile logistics*), việc các chuyến xe bị trễ giờ mà trung tâm điều hành không biết trước gây ra trễ lịch trình dây chuyền và lãng phí nguồn lực tại các kho phân phối (*hubs*).
* **Mô tả giải pháp:** **HyperLogistics-Stream** xây dựng một đường ống xử lý dữ liệu lớn theo thời gian thực (*Real-Time Big Data Pipeline*). Hệ thống thu thập luồng dữ liệu chuyến đi và thời tiết, sử dụng **PySpark Structured Streaming** và **Spark MLlib** để dự báo thời gian đến (ETA) cũng như độ trễ chuyến đi ngay khi xe đang di chuyển.
* **4 Tầng Kiến trúc Big Data:**
  1. **Ingestion (Thu thập):** **Apache Kafka** phát lại luồng sự kiện chuyến đi theo thời gian thực.
  2. **Storage (Lưu trữ):** Định dạng **Parquet** thiết kế theo mô hình **Medallion Architecture (Bronze $\rightarrow$ Silver $\rightarrow$ Gold)** trên HDFS/S3.
  3. **Analysis (Phân tích & ML):** **PySpark Structured Streaming** xử lý dữ liệu luồng; **Spark MLlib** (Gradient Boosted Trees / Random Forest) huấn luyện mô hình dự báo độ trễ.
  4. **Reporting (Hiển thị):** **FastAPI** phục vụ truy vấn và **Streamlit** hiển thị bảng điều khiển thời gian thực (*Live Dashboard*).

---

### 2. Mục Tiêu Dự Án (Project Objectives)

* **Mục tiêu nghiệp vụ (Domain Goals):**
  * Dự báo chính xác độ trễ chuyến đi ($ArrDelay$) tại thời điểm xuất bến.
  * Cảnh báo sớm các chuyến đi có nguy cơ trễ cao cho trung tâm điều hành.
  * Tính toán chỉ số mật độ chuyến đi cập bến theo từng cửa sổ thời gian (*time windows*).
* **Mục tiêu kỹ thuật Big Data (System & Benchmarking Goals):**
  * Xây dựng đường ống xử lý luồng đạt thông lượng cao (*High Throughput*) và độ trễ thấp (*Low Latency*).
  * Benchmarking hệ thống: Đo lường **Throughput (bản ghi/giây)**, **Batch Processing Time**, và **Độ trễ $p_{50}, p_{95}$** khi thay đổi:
    * Số nhân Spark CPU (2, 4, 8 cores)
    * Số lượng Shuffle Partitions (8, 64, 200)
    * Tốc độ phát sự kiện Kafka (1k, 5k, 20k events/sec)
    * Giới hạn kích thước Batch (`maxOffsetsPerTrigger`).

---

### 3. Yêu Cầu Của Thầy (Course Requirements)

Dựa trên đề cương môn học và hướng dẫn Term Project của Dr. Yusuf Sarwar:

* **Tỷ trọng điểm số Dự án (30% tổng điểm môn học):**
  * **Proposal (1 trang) & Slides thuyết trình 5 phút:** 10%
  * **Mid-term Presentation & Mid-report (3 trang - Tuần 10):** 10% + 25%
  * **Final Presentation & Final Report (6 trang - Tuần Final):** 25% + 30%
* **Quy mô Tập dữ liệu:** Tối thiểu **5 GB (lý tưởng từ 5–10 GB)** cho các dự án thuộc nhánh *Data Analysis*.
* **Cấu trúc Pipeline:** Bắt buộc thể hiện rõ 4 giai đoạn **Ingest $\rightarrow$ Store $\rightarrow$ Analyze $\rightarrow$ Report**.
* **Đánh giá Đa chiều:** Cần kết hợp cả đánh giá chất lượng mô hình (MAE, RMSE so với Baseline) lẫn hiệu năng hệ thống phân tán (Throughput, Latency, Scalability).

---

### 4. Lưu Ý Quan Trọng Trong Việc Chọn & Sử Dụng Dataset

Khi lựa chọn dataset cho dự án Big Data môn COMP-SCI 5540, cần đặc biệt lưu ý các điểm sau:

1. **Dung lượng tối thiểu ($\ge$ 5 GB):** Tập dữ liệu thô phải đạt quy mô 5–10 GB để đảm bảo bài toán thực sự cần đến các công cụ tính toán phân tán như Hadoop/Spark.
2. **Cấu trúc & Độ chi tiết (Granularity):**
   * Dự án Real-Time Streaming/Analytics đòi hỏi dữ liệu dạng **sự kiện từng chuyến đi (event-level / time-series logs)** có mốc thời gian xuất phát, cập bến chi tiết.
   * **Tránh dùng:** Các tập dữ liệu thống kê vĩ mô tổng hợp theo năm (như FAF5), vì không thể cắt nhỏ thành luồng sự kiện thời gian thực cho Kafka.
3. **Sử dụng Dataset Đại diện (Proxy Dataset) hợp lý:**
   * Dữ liệu GPS/telematics xe tải thực tế quy mô 5–10 GB thường là tài sản bảo mật thương mại của doanh nghiệp.
   * **Giải pháp:** Sử dụng dữ liệu **BTS On-Time Performance (2018–2024, 5–10 GB)** kết hợp với **NOAA Weather Data** làm tập dữ liệu đại diện kỹ thuật (*technical proxy*). Lịch trình cất/hạ cánh và thời tiết của chuyến bay có cấu trúc tương đồng hoàn toàn với hành trình giữa các trạm logistics.
4. **Mở rộng kết hợp dữ liệu đường bộ (Road Transport Extensions):**
   * Để tăng tính thực tế cho bài toán xe tải, có thể kết hợp thêm các tập dữ liệu mở đường bộ như **US Accidents Dataset (7.7M bản ghi tai nạn/ùn tắc cao tốc Mỹ)** hoặc **NYC TLC Trip Data** / **Caltrans PeMS**.

---

### 5. Lộ Trình Thực Hiện 8 Tuần (Milestone Roadmap)

* **Tuần 1:** Proposal, thiết lập Repository, Docker & Kafka cluster.
* **Tuần 1–2:** Tải dữ liệu BTS & NOAA, làm sạch và kết nối (*Join*).
* **Tuần 2–4:** Trích xuất đặc trưng (*Feature Engineering*), huấn luyện mô hình Spark MLlib & xây dựng Baselines.
* **Tuần 2–5:** Xây dựng Kafka Producer & Spark Structured Streaming job.
* **Tuần 4–6:** Phát triển FastAPI backend & Streamlit live dashboard.
* **Tuần 5–7:** Chạy thực nghiệm Benchmarking cấu hình Spark & phân tích kết quả.
* **Tuần 7–8:** Hoàn thiện Final Report, Slide thuyết trình và chuẩn bị Demo.
