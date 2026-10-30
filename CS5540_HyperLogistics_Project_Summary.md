# CS 5540 Principles of Big Data Management
## Tổng Quan Dự Án, Mục Tiêu, Yêu Cầu & Kiến Trúc Hệ Thống (Plan V2)

**Môn học:** COMP-SCI 5540 Principles of Big Data Management (Fall 2026, UMKC)  
**Giảng viên:** Dr. Md Yusuf Sarwar Uddin  
**Nhóm thực hiện:** Tony Nguyen (`tmnc62@umsystem.edu`) & Joe Doan (`jdbc6@umsystem.edu`)  
**Tên dự án:** HyperLogistics-Stream (Real-Time Hub-to-Hub ETA Prediction with Kafka and Spark Structured Streaming)  

---

### 1. Tóm Tắt Dự Án (Project Summary)

* **Bối cảnh & Vấn đề:** Trong vận tải chặng giữa (*middle-mile freight logistics*), việc các chuyến bay/hành trình vận tải bị chậm trễ ngoài dự kiến gây tắc nghẽn nghiêm trọng tại các cổng tiếp nhận (*dock doors*) và trung tâm điều phối trung chuyển (*distribution hubs*).
* **Mô tả giải pháp:** **HyperLogistics-Stream** là hệ thống xử lý dữ liệu lớn phân tán thời gian thực (*Distributed Real-Time Big Data Pipeline*) kết hợp hai chế độ:
  1. **Historical Replay Engine:** Tái hiện luồng hành trình lịch sử theo thứ tự thời gian cất cánh (*WheelsOff*) ở tốc độ cao (1k, 5k, 20k sự kiện/giây) phục vụ huấn luyện, suy luận streaming và đo kiểm năng lực chịu tải (*stress benchmark*).
  2. **Live Production Stream & Radar (Plan V2):** Thu thập dữ liệu thực tế liên tục từ các API công cộng (OpenSky Network, FAA NAS Status, NWS METAR Weather), phân tích áp lực các hub hàng không/vận tải chính và so sánh hiện trạng so với mức cơ sở lịch sử (*Today vs. Typical baseline*).
* **4 Tầng Kiến trúc Big Data:**
  1. **Ingestion (Thu thập):** **Apache Kafka** quản lý luồng sự kiện đa nguồn (`flights_stream`, `live_flights`, `airport_status`, `weather`).
  2. **Storage (Lưu trữ Medallion):** Định dạng **Parquet** tối ưu hóa trên HDFS/S3/Local:
     * **Bronze:** Bản ghi thô từ Kafka kèm checkpoint.
     * **Silver:** Dữ liệu đã làm sạch và kết nối tĩnh-luồng (*stream-static broadcast join*) với thông tin trạm thời tiết NOAA/NWS.
     * **Gold:** Bảng dự báo ETA động (`gold/predictions`), tổng hợp áp lực hub theo cửa sổ trượt (`gold/hub_congestion`), và trạng thái live hub (`gold/live_hub_status`).
  3. **Analysis & ML (Phân tích & Máy học):**
     * **Spark MLlib** (`GBTRegressor`, `RandomForestRegressor`) huấn luyện offline trên tập dữ liệu 2022–2023 (`pipeline/01_batch_train.py`).
     * **PySpark Structured Streaming** tính toán suy luận ETA thời gian thực (`pipeline/03_stream_processor.py` cho Replay và `pipeline/04_live_processor.py` cho Live Feeds).
  4. **Serving & Reporting (Phục vụ & Trực quan hóa):**
     * **FastAPI** (`api/main.py`): REST microservice cung cấp API truy vấn độ trễ và số liệu streaming.
     * **Streamlit Operational Dashboard** (`dashboard/app.py`, `dashboard/live_radar.py`): Gồm **Live Radar** (bản đồ PyDeck/Plotly hiển thị các hub đang chịu tải, cảnh báo FAA, thời tiết, vị trí máy bay), **Replay & Benchmark** (đo kiểm RQ2), và **Story tabs** ("Why delays happen", làn đường trễ nhất, tác động thời tiết).

---

### 2. Mục Tiêu Dự Án (Project Objectives)

* **Mục tiêu nghiệp vụ & Kể chuyện dữ liệu (Domain & Storytelling Goals):**
  * Dự báo độ biến thiên độ trễ đến: $\Delta\text{delay} = \text{ArrDelay} - \text{DepDelay}$ và cập nhật ETA động khi chuyến bay đang di chuyển.
  * Phân loại và cảnh báo sớm các chuyến đi có nguy cơ trễ cao (`is_high_risk`, $\Delta\text{delay} > 15$ hoặc $30$ phút).
  * Kể câu chuyện vận hành trực quan cho điều độ viên: *"Hub DFW đang trễ hơn 18 phút so với thường lệ của tối thứ Năm vì gió giật và lệnh hoãn cất cánh của FAA"*.
* **Mục tiêu kỹ thuật Big Data (System & Benchmarking Goals - RQ2):**
  * Đạt năng lực xử lý phân tán thông lượng cao (*High Throughput*) và độ trễ đuôi thấp (*Low Latency*).
  * Thực nghiệm kiểm chuẩn trên lưới tham số $3 \times 3 \times 3 \times 3$:
    * Số nhân Spark CPU (2, 4, 8 cores).
    * Số lượng Shuffle Partitions (8, 64, 200).
    * Tốc độ phát Kafka (1,000, 5,000, 20,000 sự kiện/giây).
    * Ngưỡng kích thước Micro-batch (`maxOffsetsPerTrigger`: 5k, 20k, 100k).
  * Đo lường chính xác Throughput (recs/sec), Batch Processing Time (ms), và $p_{50}, p_{95}$ End-to-End Latency.

---

### 3. Yêu Cầu Môn Học & Đánh Giá (Course Requirements)

* **Tỷ trọng điểm số Dự án (Term Project - COMP-SCI 5540):**
  * **Proposal & Initial Pitch:** 10%
  * **Mid-term Presentation & Mid-report:** 10% + 25%
  * **Final Presentation, Final Report & Live Demo:** 25% + 30%
* **Quy mô Tập dữ liệu:** Tối thiểu **5 GB** (Dự án sử dụng **~11 GB** dữ liệu thực tế: BTS ~9.8 GB + NOAA ~911 MB).
* **Cấu trúc Pipeline hoàn chỉnh:** Đảm bảo đủ 4 giai đoạn **Ingest $\rightarrow$ Store $\rightarrow$ Analyze $\rightarrow$ Report**.
* **Đánh giá Đa chiều:** Đánh giá chất lượng mô hình (MAE, RMSE so với Baselines) song song với hiệu năng hệ thống streaming (Throughput, Latency, Scalability, Fault-tolerance).

---

### 4. Lựa Chọn Dataset & Định Hướng Đại Diện Kỹ Thuật (Technical Proxy)

1. **Quy mô dữ liệu thực tế (~11 GB):**
   * **BTS Airline On-Time Performance (2022–2024, ~9.8 GB):**
     * 2022–2023 (~6.5 GB): Phân vùng huấn luyện mô hình MLlib và thiết lập chỉ số trễ trung bình lịch sử các chặng bay.
     * 2024 (~3.3 GB): Phân vùng tái hiện sự kiện luồng cho Kafka.
   * **NOAA ISD Surface Weather Hourly (2022–2024, ~911 MB):**
     * Dữ liệu quan trắc bề mặt (tốc độ gió, tầm nhìn, nhiệt độ) tại top 40 hub logistics và hàng không trọng điểm của Mỹ.
2. **Khẳng định tính chất Proxy (Public Proxy for Middle-Mile Freight):**
   * Dữ liệu telematics xe tải thực tế quy mô lớn là tài sản bảo mật thương mại.
   * Chuyến bay vận tải và hành khách liên hub có cấu trúc mốc thời gian (Scheduled Departure, Wheels-Off, Scheduled Arrival, Wheels-On), tọa độ địa lý trạm trung chuyển, và sự phụ thuộc thời tiết hoàn toàn tương đồng với hành trình xe tải đường dài.
   * Theo định hướng của Giáo viên hướng dẫn: Tập trung khai thác sâu dữ liệu hàng không làm đại diện kỹ thuật chuẩn mực, kết hợp nguồn dữ liệu thời gian thực (FAA NAS + NWS + OpenSky) để tạo ra các câu chuyện dữ liệu có giá trị vận hành thực tiễn.

---

### 5. Lộ Trình & Hiện Trạng Thực Hiện (Roadmap & Status)

| Giai đoạn | Nội dung công việc | Hiện trạng |
|---|---|:---:|
| **Phase 1** | Tải và làm sạch dữ liệu BTS & NOAA, trích xuất đặc trưng, huấn luyện Spark MLlib (`pipeline/01_batch_train.py`). | ✅ Đã hoàn thành |
| **Phase 2** | Xây dựng Kafka Replay Producer (`pipeline/02_kafka_producer.py`) và Live Feed Producer đa nguồn (`live/live_producer.py`). | ✅ Đã hoàn thành |
| **Phase 3** | Xây dựng Spark Structured Streaming (`pipeline/03_stream_processor.py`) và Spark Live Stream Processor (`pipeline/04_live_processor.py`). | ✅ Đã hoàn thành |
| **Phase 4** | Thiết lập kho lưu trữ Parquet Medallion (Bronze/Silver/Gold) và bộ đo kiểm hiệu năng phân tán RQ2 (`benchmark/run_benchmark.py`). | ✅ Đã hoàn thành |
| **Phase 5** | Phát triển FastAPI (`api/main.py`), Live Radar Map & Replay Dashboard (`dashboard/app.py`, `dashboard/live_radar.py`). | ✅ Đã hoàn thành |
| **Final Delivery**| Kiểm thử Fresh-Clone, hoàn thiện Poster, Video thuyết trình 3 phút và Báo cáo tổng kết. | 🔄 Đang triển khai |
