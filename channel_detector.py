"""
========================================
MODULE: CHANNEL DETECTOR (Phần 1/7)
========================================

MỤC ĐÍCH:
---------
Phát hiện kênh giá (channel) từ các điểm đỉnh/đáy do ZigZag tìm được.
Tìm tổ hợp 2 đường (trên + dưới) đẹp nhất để sẵn sàng vào lệnh.

PHẦN 1: CẤU TRÚC CƠ BẢN + QUẢN LÝ DỮ LIỆU
-----------------------------------------
- Nhận điểm đỉnh/đáy từ ZigZag
- Lưu trữ vào danh sách (sliding window)
- Xóa điểm cũ khi vượt quá giới hạn
"""


class ChannelDetector:
    """
    Lớp phát hiện kênh giá từ các điểm đỉnh/đáy
    """
    
    def __init__(self, max_pivots=10, max_age_ms=None, H=5000, point=0.1,
                 max_slope=None, min_distance_candles=3,
                 max_penetration_pct=0.3, max_penetrating_candles=2):
        """
        Khởi tạo ChannelDetector
        
        THAM SỐ:
        --------
        max_pivots: int
            Số điểm tối đa giữ trong mỗi danh sách đỉnh/đáy (sliding window)
            Ví dụ: 10 = giữ 10 đỉnh gần nhất và 10 đáy gần nhất
            
        max_age_ms: int hoặc None
            Tuổi tối đa của điểm (tính bằng milliseconds)
            Nếu None: chỉ dùng max_pivots để giới hạn
            Nếu có giá trị: xóa điểm cũ hơn max_age_ms
            Ví dụ: 86400000 = 24 giờ (24*60*60*1000)
            
        H: int
            Tham số cữ từ ZigZag (dùng để tính độ rộng kênh)
            
        point: float
            Giá trị 1 pip (dùng để tính dH = H * point)
            
        max_slope: float hoặc None
            Độ dốc tối đa cho phép (giá trị tuyệt đối)
            Nếu |slope| > max_slope → Loại đường (quá dốc)
            Nếu None: không giới hạn độ dốc
            Ví dụ: 0.001 = giới hạn độ dốc
            
        min_distance_candles: int
            Khoảng cách tối thiểu giữa 2 điểm (tính bằng số nến)
            Dùng để loại bỏ đường tạo từ 2 điểm quá gần nhau
            Ví dụ: 3 = 2 điểm phải cách nhau ít nhất 3 nến
            
        max_penetration_pct: float
            % phá qua tối đa cho phép (ví dụ: 0.3 = 0.3%)
            Nếu nến phá > % này → Loại đường
            
        max_penetrating_candles: int
            Số nến phá qua tối đa cho phép (ví dụ: 2)
            Nếu có > 2 nến phá qua → Loại đường
        """
        # ===== THAM SỐ CẤU HÌNH =====
        self.max_pivots = max_pivots
        self.max_age_ms = max_age_ms
        self.H = H
        self.point = point
        self.dH = H * point  # Độ rộng cữ giá chuẩn
        
        # Tham số validation (PHẦN 3)
        self.max_slope = max_slope
        self.min_distance_candles = min_distance_candles
        
        # Tham số penetration analysis
        self.max_penetration_pct = max_penetration_pct
        self.max_penetrating_candles = max_penetrating_candles
        
        # Ước lượng khoảng cách 1 nến (milliseconds)
        # Giả định timeframe trung bình là 15 phút = 900000 ms
        self.candle_interval_ms = 15 * 60 * 1000
        
        # ===== LƯU TRỮ ĐIỂM ĐỈNH/ĐÁY =====
        self.peaks_list = []      # Danh sách các đỉnh: [{timestamp, price, index_id}, ...]
        self.troughs_list = []    # Danh sách các đáy: [{timestamp, price, index_id}, ...]
        
        # ===== LƯU TRỮ CANDLES (SLIDING WINDOW) =====
        self.candles_list = []    # Danh sách các nến: [{timestamp, open, high, low, close, volume}, ...]
        
        # index_id để đánh số thứ tự điểm (tăng dần, không bao giờ reset)
        # Dùng để tránh tạo lại đường cũ
        self._next_pivot_id = 0
        
        # ===== LƯU TRỮ ĐƯỜNG THẲNG (sẽ dùng ở phần sau) =====
        self.upper_lines = []     # Danh sách đường trên đã validate
        self.lower_lines = []     # Danh sách đường dưới đã validate
        
        # ===== LƯU TRỮ TỔ HỢP (sẽ dùng ở phần sau) =====
        self.valid_combinations = []  # Danh sách tổ hợp (upper, lower) đã validate
        
        # ===== THEO DÕI ĐÃ TẠO (tránh tạo lại) =====
        self.created_line_pairs = set()  # Set các cặp (id1, id2) đã tạo đường
        self.created_combinations = set()  # Set các cặp (upper_id, lower_id) đã tạo tổ hợp
        
        print(f"✓ Khởi tạo ChannelDetector: max_pivots={max_pivots}, "
              f"max_age_ms={max_age_ms}, H={H}, point={point}, dH={self.dH}")
        print(f"  Validation: max_slope={max_slope}, min_distance_candles={min_distance_candles}")
        print(f"  Penetration: max_pct={max_penetration_pct}%, max_candles={max_penetrating_candles}")
    
    
    def add_pivot(self, zigzag_output):
        """
        Thêm điểm đỉnh/đáy mới từ ZigZag
        
        INPUT:
        ------
        zigzag_output: dict
            {
                'candle': {
                    'timestamp': '2025-01-15 10:30:00',
                    'open': 95000,
                    'high': 95500,
                    'low': 94800,
                    'close': 95200,
                    'volume': 1234
                },
                'pivot': {
                    'timestamp': 1731758700000,  # Unix timestamp (milliseconds)
                    'price': 95000.5,
                    'type': 'peak'  # hoặc 'trough'
                } hoặc None
            }
        
        XỬ LÝ:
        ------
        1. Bỏ qua nếu pivot là None
        2. Gán index_id cho điểm mới
        3. Thêm vào danh sách tương ứng (peaks hoặc troughs)
        4. Xóa điểm cũ nếu vượt quá giới hạn
        5. Tự động tạo đường mới từ pivot này (PHẦN 2)
        
        RETURN:
        -------
        pivot_id: int hoặc None
            ID của điểm vừa thêm vào (để theo dõi)
            None nếu pivot là None
        """
        # Lấy candle và pivot từ output
        candle = zigzag_output.get('candle')
        pivot = zigzag_output.get('pivot')
        
        # Luôn lưu candle vào sliding window (dù có pivot hay không)
        if candle:
            self.candles_list.append(candle)
        
        # Nếu không có pivot, bỏ qua phần xử lý pivot
        if pivot is None:
            return None
        # Gán ID cho điểm mới
        pivot_id = self._next_pivot_id
        self._next_pivot_id += 1
        
        # Tạo điểm với đầy đủ thông tin
        pivot_full = {
            'id': pivot_id,
            'timestamp': pivot['timestamp'],
            'price': pivot['price'],
            'type': pivot['type']
        }
        
        # Phân loại và thêm vào danh sách tương ứng
        if pivot['type'] == 'peak':
            self.peaks_list.append(pivot_full)
            list_name = 'đỉnh'
        else:  # trough
            self.troughs_list.append(pivot_full)
            list_name = 'đáy'
        
        print(f"  → Thêm {list_name} #{pivot_id}: "
              f"Price={pivot['price']}, Timestamp={pivot['timestamp']}")
        
        # Xóa điểm cũ nếu cần
        self._cleanup_old_pivots()
        
        # Tạo đường mới từ pivot này (PHẦN 2)
        self._generate_new_lines(pivot_id)
        
        return pivot_id
    
    
    def _cleanup_old_pivots(self):
        """
        Xóa các điểm đỉnh/đáy cũ theo 2 tiêu chí:
        1. Vượt quá số lượng tối đa (max_pivots)
        2. Quá cũ về thời gian (max_age_ms)
        
        XỬ LÝ:
        ------
        - Xóa điểm cũ nhất nếu len(list) > max_pivots
        - Xóa điểm có timestamp < (timestamp_mới_nhất - max_age_ms)
        """
        removed_ids = []
        
        # ===== XỬ LÝ DANH SÁCH ĐỈNH =====
        if len(self.peaks_list) > 0:
            # Xóa theo số lượng
            while len(self.peaks_list) > self.max_pivots:
                removed = self.peaks_list.pop(0)  # Xóa điểm cũ nhất (đầu danh sách)
                removed_ids.append(removed['id'])
                print(f"  ✗ Xóa đỉnh #{removed['id']} (vượt quá {self.max_pivots} điểm)")
            
            # Xóa theo tuổi (nếu có max_age_ms)
            if self.max_age_ms is not None:
                latest_timestamp = self.peaks_list[-1]['timestamp']  # Timestamp mới nhất
                cutoff_timestamp = latest_timestamp - self.max_age_ms
                
                # Lọc bỏ điểm quá cũ
                old_peaks = [p for p in self.peaks_list if p['timestamp'] < cutoff_timestamp]
                for old_peak in old_peaks:
                    self.peaks_list.remove(old_peak)
                    removed_ids.append(old_peak['id'])
                    print(f"  ✗ Xóa đỉnh #{old_peak['id']} (quá cũ)")
        
        # ===== XỬ LÝ DANH SÁCH ĐÁY =====
        if len(self.troughs_list) > 0:
            # Xóa theo số lượng
            while len(self.troughs_list) > self.max_pivots:
                removed = self.troughs_list.pop(0)
                removed_ids.append(removed['id'])
                print(f"  ✗ Xóa đáy #{removed['id']} (vượt quá {self.max_pivots} điểm)")
            
            # Xóa theo tuổi
            if self.max_age_ms is not None:
                latest_timestamp = self.troughs_list[-1]['timestamp']
                cutoff_timestamp = latest_timestamp - self.max_age_ms
                
                old_troughs = [t for t in self.troughs_list if t['timestamp'] < cutoff_timestamp]
                for old_trough in old_troughs:
                    self.troughs_list.remove(old_trough)
                    removed_ids.append(old_trough['id'])
                    print(f"  ✗ Xóa đáy #{old_trough['id']} (quá cũ)")
        
        # Nếu có điểm bị xóa, xóa các đường/tổ hợp liên quan
        if len(removed_ids) > 0:
            self._cleanup_old_lines(removed_ids)
            # Xóa candles cũ tương ứng
            self._cleanup_old_candles()
    
    
    def _cleanup_old_candles(self):
        """
        Xóa candles cũ để đồng bộ với pivots
        
        MỤC ĐÍCH:
        ---------
        Khi xóa pivot cũ, cũng xóa các candles trước pivot cũ nhất còn lại
        
        XỬ LÝ:
        ------
        1. Tìm timestamp của pivot cũ nhất (trong cả peaks và troughs)
        2. Xóa tất cả candles có timestamp < pivot_oldest_timestamp
        """
        if len(self.candles_list) == 0:
            return
        
        # Tìm timestamp pivot cũ nhất
        oldest_pivot_ts = None
        
        if len(self.peaks_list) > 0 and len(self.troughs_list) > 0:
            oldest_pivot_ts = min(self.peaks_list[0]['timestamp'], self.troughs_list[0]['timestamp'])
        elif len(self.peaks_list) > 0:
            oldest_pivot_ts = self.peaks_list[0]['timestamp']
        elif len(self.troughs_list) > 0:
            oldest_pivot_ts = self.troughs_list[0]['timestamp']
        
        if oldest_pivot_ts is None:
            return
        
        # Chuyển đổi timestamp để so sánh
        def to_ms(ts):
            if isinstance(ts, (int, float)):
                return int(ts)
            try:
                import pandas as pd
                return int(pd.to_datetime(ts).timestamp() * 1000)
            except:
                return 0
        
        oldest_pivot_ms = to_ms(oldest_pivot_ts)
        
        # Xóa candles cũ hơn pivot cũ nhất
        initial_count = len(self.candles_list)
        self.candles_list = [c for c in self.candles_list if to_ms(c['timestamp']) >= oldest_pivot_ms]
        removed_count = initial_count - len(self.candles_list)
        
        if removed_count > 0:
            print(f"  ✗ Xóa {removed_count} nến cũ (trước pivot cũ nhất)")
    
    
    # ========================================
    # PHẦN 2: TẠO ĐƯỜNG THẲNG
    # ========================================
    
    def create_line(self, point1, point2):
        """
        Tạo phương trình đường thẳng từ 2 điểm
        
        CÔNG THỨC:
        ----------
        Đường thẳng: y = a*x + b
        Với x = timestamp, y = price
        
        a (độ dốc) = (y2 - y1) / (x2 - x1)
        b (điểm cắt) = y1 - a*x1
        
        INPUT:
        ------
        point1, point2: dict
            {
                'id': 0,
                'timestamp': 1700000000000,
                'price': 95000,
                'type': 'peak' hoặc 'trough'
            }
        
        OUTPUT:
        -------
        line: dict
            {
                'id': 'line_0_2',           # ID duy nhất = 'line_{id1}_{id2}'
                'point1': {...},            # Điểm 1
                'point2': {...},            # Điểm 2
                'slope': 0.05,              # Độ dốc (a)
                'intercept': 10000,         # Điểm cắt (b)
                'type': 'upper'/'lower',    # Đường trên hay dưới
                'point_ids': (0, 2)         # Tuple IDs của 2 điểm (để theo dõi)
            }
        """
        # Tính độ dốc (slope)
        x1 = point1['timestamp']
        y1 = point1['price']
        x2 = point2['timestamp']
        y2 = point2['price']
        
        # Tránh chia cho 0 (2 điểm trùng timestamp - không nên xảy ra)
        if x2 == x1:
            print(f"  ⚠ Cảnh báo: 2 điểm có cùng timestamp! Bỏ qua.")
            return None
        
        slope = (y2 - y1) / (x2 - x1)
        intercept = y1 - slope * x1
        
        # Xác định loại đường (upper hay lower)
        line_type = 'upper' if point1['type'] == 'peak' else 'lower'
        
        # Tạo ID duy nhất cho đường
        line_id = f"line_{point1['id']}_{point2['id']}"
        
        line = {
            'id': line_id,
            'point1': point1,
            'point2': point2,
            'slope': slope,
            'intercept': intercept,
            'type': line_type,
            'point_ids': (point1['id'], point2['id'])
        }
        
        return line
    
    
    def _generate_new_lines(self, new_pivot_id):
        """
        Tạo các đường mới từ pivot vừa thêm vào
        
        NGUYÊN TẮC:
        -----------
        - Chỉ tạo đường kết hợp pivot MỚI với các pivot CŨ cùng loại
        - KHÔNG tạo lại các đường đã tồn tại
        - Kiểm tra xem cặp (id1, id2) đã tạo chưa bằng created_line_pairs
        
        INPUT:
        ------
        new_pivot_id: int
            ID của pivot vừa thêm vào
        
        XỬ LÝ:
        ------
        1. Tìm pivot mới trong danh sách (peaks hoặc troughs)
        2. Lấy danh sách các pivot cũ cùng loại
        3. Tạo đường kết hợp pivot mới với từng pivot cũ
        4. Lưu vào created_line_pairs để tránh tạo lại
        """
        # Tìm pivot mới
        new_pivot = None
        pivot_list = None
        
        # Tìm trong danh sách đỉnh
        for p in self.peaks_list:
            if p['id'] == new_pivot_id:
                new_pivot = p
                pivot_list = self.peaks_list
                break
        
        # Nếu không có trong đỉnh, tìm trong đáy
        if new_pivot is None:
            for t in self.troughs_list:
                if t['id'] == new_pivot_id:
                    new_pivot = t
                    pivot_list = self.troughs_list
                    break
        
        # Không tìm thấy pivot (không nên xảy ra)
        if new_pivot is None:
            return
        
        # Tạo đường với các pivot cũ cùng loại
        new_lines = []
        
        for old_pivot in pivot_list:
            # Bỏ qua chính nó
            if old_pivot['id'] == new_pivot_id:
                continue
            
            # Tạo cặp ID có thứ tự (nhỏ, lớn) để kiểm tra
            pair = tuple(sorted([old_pivot['id'], new_pivot['id']]))
            
            # Kiểm tra đã tạo chưa
            if pair in self.created_line_pairs:
                continue  # Đã tạo rồi, bỏ qua
            
            # Tạo đường mới
            line = self.create_line(old_pivot, new_pivot)
            
            if line is not None:
                new_lines.append(line)
                # Đánh dấu đã tạo
                self.created_line_pairs.add(pair)
                print(f"    ✓ Tạo đường {line['type']}: {line['id']} "
                      f"(độ dốc={line['slope']:.6f})")
        
        # Phân loại và lưu vào danh sách tương ứng
        for line in new_lines:
            if line['type'] == 'upper':
                self.upper_lines.append(line)
            else:
                self.lower_lines.append(line)
        
        # Validate tất cả đường sau khi thêm mới (PHẦN 3)
        if len(new_lines) > 0:
            self._update_valid_lines()
    
    
    def _cleanup_old_lines(self, removed_pivot_ids):
        """
        Xóa các đường chứa pivot đã bị xóa
        
        INPUT:
        ------
        removed_pivot_ids: list[int]
            Danh sách IDs của các pivot đã bị xóa
        
        XỬ LÝ:
        ------
        1. Duyệt qua upper_lines và lower_lines
        2. Nếu đường chứa pivot đã bị xóa → Xóa đường
        3. Xóa khỏi created_line_pairs
        """
        # Chuyển thành set để kiểm tra nhanh
        removed_ids_set = set(removed_pivot_ids)
        
        # Xóa các đường trên
        lines_to_remove = []
        for line in self.upper_lines:
            # Kiểm tra nếu đường chứa pivot đã bị xóa
            if line['point1']['id'] in removed_ids_set or line['point2']['id'] in removed_ids_set:
                lines_to_remove.append(line)
        
        for line in lines_to_remove:
            self.upper_lines.remove(line)
            # Xóa khỏi created_line_pairs
            self.created_line_pairs.discard(line['point_ids'])
            print(f"    ✗ Xóa đường trên: {line['id']}")
        
        # Xóa các đường dưới
        lines_to_remove = []
        for line in self.lower_lines:
            if line['point1']['id'] in removed_ids_set or line['point2']['id'] in removed_ids_set:
                lines_to_remove.append(line)
        
        for line in lines_to_remove:
            self.lower_lines.remove(line)
            self.created_line_pairs.discard(line['point_ids'])
            print(f"    ✗ Xóa đường dưới: {line['id']}")
    
    
    # ========================================
    # PHẦN 3: VALIDATION ĐƠN
    # ========================================
    
    def validate_single_line(self, line):
        """
        Kiểm tra đường có hợp lệ không (validation đơn)
        
        TIÊU CHÍ KIỂM TRA:
        ------------------
        1. Độ dốc (slope):
           - Nếu |slope| > max_slope → Loại (quá dốc, thị trường biến động mạnh)
           
        2. Khoảng cách giữa 2 điểm:
           - Nếu khoảng cách < min_distance_candles → Loại (2 điểm quá gần, không tin cậy)
           - Tính: distance = |timestamp2 - timestamp1| / candle_interval_ms
        
        3. Penetration analysis:
           - Kiểm tra các nến giữa 2 pivot có phá qua đường không
           - Nếu có > max_penetrating_candles nến phá → Loại
           - Nếu có nến phá > max_penetration_pct % → Loại
        
        INPUT:
        ------
        line: dict
            Đường cần kiểm tra (từ create_line)
        
        OUTPUT:
        -------
        valid: bool
            True = Hợp lệ, False = Không hợp lệ
        reason: str
            Lý do nếu không hợp lệ (dùng để debug)
        """
        # ===== KIỂM TRA 1: ĐỘ DỐC =====
        if self.max_slope is not None:
            if abs(line['slope']) > self.max_slope:
                reason = f"Độ dốc quá lớn: |{line['slope']:.6f}| > {self.max_slope}"
                return False, reason
        
        # ===== KIỂM TRA 2: KHOẢNG CÁCH GIỮA 2 ĐIỂM =====
        timestamp1 = line['point1']['timestamp']
        timestamp2 = line['point2']['timestamp']
        time_distance_ms = abs(timestamp2 - timestamp1)
        
        # Ước lượng số nến giữa 2 điểm
        num_candles = time_distance_ms / self.candle_interval_ms
        
        if num_candles < self.min_distance_candles:
            reason = f"2 điểm quá gần: {num_candles:.1f} nến < {self.min_distance_candles}"
            return False, reason
        
        # ===== KIỂM TRA 3: PENETRATION ANALYSIS =====
        is_valid, reason = self._check_line_penetration(line)
        if not is_valid:
            return False, reason
        
        # ===== TẤT CẢ ĐIỀU KIỆN ĐỀU OK =====
        return True, "OK"
    
    
    def _check_line_penetration(self, line):
        """
        Kiểm tra các nến có phá qua đường không
        
        MỤC ĐÍCH:
        ---------
        Phân tích các nến nằm giữa 2 pivot tạo đường để xem có nến nào
        phá qua đường không. Đếm số nến phá và mức độ phá của từng nến.
        
        TIÊU CHÍ LOẠI:
        --------------
        1. Có > max_penetrating_candles nến phá qua
        2. Có bất kỳ nến nào phá > max_penetration_pct %
        
        INPUT:
        ------
        line: dict
            Đường cần kiểm tra
            
        OUTPUT:
        -------
        valid: bool
            True = Không có penetration vượt ngưỡng
            False = Có penetration vượt ngưỡng
        reason: str
            Lý do nếu không hợp lệ
        """
        if len(self.candles_list) == 0:
            return True, "OK"
        
        # Chuyển đổi timestamp
        def to_ms(ts):
            if isinstance(ts, (int, float)):
                return int(ts)
            try:
                import pandas as pd
                return int(pd.to_datetime(ts).timestamp() * 1000)
            except:
                return 0
        
        # Lấy khoảng thời gian của đường
        ts1 = line['point1']['timestamp']
        ts2 = line['point2']['timestamp']
        start_ts = min(ts1, ts2)
        end_ts = max(ts1, ts2)
        
        # Lọc các nến nằm giữa 2 pivot (không bao gồm 2 pivot)
        candles_between = []
        for candle in self.candles_list:
            candle_ts = to_ms(candle['timestamp'])
            if start_ts < candle_ts < end_ts:
                candles_between.append(candle)
        
        # Nếu không có nến giữa 2 pivot, OK
        if len(candles_between) == 0:
            return True, "OK"
        
        # Xác định loại đường (upper/lower)
        line_type = line['type']  # 'upper' hoặc 'lower'
        
        # Đếm số nến phá và mức độ phá
        penetrating_candles = []
        
        for candle in candles_between:
            candle_ts = to_ms(candle['timestamp'])
            
            # Tính giá của đường tại thời điểm nến này
            # line_price = slope * timestamp + intercept
            line_price = line['slope'] * candle_ts + line['intercept']
            
            # Kiểm tra penetration
            if line_type == 'upper':
                # Đường trên: kiểm tra high có vượt qua đường không
                if candle['high'] > line_price:
                    penetration_pct = ((candle['high'] - line_price) / line_price) * 100
                    penetrating_candles.append({
                        'timestamp': candle['timestamp'],
                        'penetration_pct': penetration_pct
                    })
            else:  # lower
                # Đường dưới: kiểm tra low có thủng xuống dưới đường không
                if candle['low'] < line_price:
                    penetration_pct = ((line_price - candle['low']) / line_price) * 100
                    penetrating_candles.append({
                        'timestamp': candle['timestamp'],
                        'penetration_pct': penetration_pct
                    })
        
        # Kiểm tra tiêu chí 1: Số nến phá
        if len(penetrating_candles) > self.max_penetrating_candles:
            reason = f"Quá nhiều nến phá: {len(penetrating_candles)} > {self.max_penetrating_candles}"
            return False, reason
        
        # Kiểm tra tiêu chí 2: Mức độ phá
        for pen in penetrating_candles:
            if pen['penetration_pct'] > self.max_penetration_pct:
                reason = f"Nến phá quá mạnh: {pen['penetration_pct']:.2f}% > {self.max_penetration_pct}%"
                return False, reason
        
        return True, "OK"
    
    
    def _update_valid_lines(self):
        """
        Lọc và cập nhật danh sách đường hợp lệ
        
        MỤC ĐÍCH:
        ---------
        Sau khi tạo đường mới hoặc xóa đường cũ, cần validate lại
        để đảm bảo chỉ giữ các đường hợp lệ trong upper_lines và lower_lines
        
        XỬ LÝ:
        ------
        1. Duyệt qua upper_lines, kiểm tra từng đường
        2. Giữ lại đường hợp lệ, xóa đường không hợp lệ
        3. Tương tự với lower_lines
        
        LƯU Ý:
        ------
        Hàm này được gọi tự động từ _generate_new_lines
        """
        # ===== LỌC ĐƯỜNG TRÊN =====
        valid_upper = []
        for line in self.upper_lines:
            is_valid, reason = self.validate_single_line(line)
            if is_valid:
                valid_upper.append(line)
            else:
                print(f"    ✗ Loại đường trên {line['id']}: {reason}")
                # Xóa khỏi created_line_pairs
                self.created_line_pairs.discard(line['point_ids'])
        
        self.upper_lines = valid_upper
        
        # ===== LỌC ĐƯỜNG DƯỚI =====
        valid_lower = []
        for line in self.lower_lines:
            is_valid, reason = self.validate_single_line(line)
            if is_valid:
                valid_lower.append(line)
            else:
                print(f"    ✗ Loại đường dưới {line['id']}: {reason}")
                # Xóa khỏi created_line_pairs
                self.created_line_pairs.discard(line['point_ids'])
        
        self.lower_lines = valid_lower
    
    
    def reset(self):
        """
        Reset toàn bộ trạng thái về ban đầu
        
        MỤC ĐÍCH:
        ---------
        Xóa toàn bộ dữ liệu đã lưu, bắt đầu lại từ đầu
        """
        self.peaks_list = []
        self.troughs_list = []
        self.candles_list = []
        self.upper_lines = []
        self.lower_lines = []
        self.valid_combinations = []
        self.created_line_pairs = set()
        self.created_combinations = set()
        self._next_pivot_id = 0
        
        print("✓ Đã reset toàn bộ trạng thái ChannelDetector")
    
    
    def get_state(self):
        """
        Lấy thông tin trạng thái hiện tại (dùng để debug)
        
        RETURN:
        -------
        dict chứa thông tin trạng thái
        """
        return {
            'num_peaks': len(self.peaks_list),
            'num_troughs': len(self.troughs_list),
            'num_candles': len(self.candles_list),
            'num_upper_lines': len(self.upper_lines),
            'num_lower_lines': len(self.lower_lines),
            'num_combinations': len(self.valid_combinations),
            'next_pivot_id': self._next_pivot_id,
            'peaks_ids': [p['id'] for p in self.peaks_list],
            'troughs_ids': [t['id'] for t in self.troughs_list]
        }


# ========================================
# TEST PHẦN 1
# ========================================

if __name__ == "__main__":
    print("=" * 60)
    print("TEST PHẦN 1+2+3: CƠ BẢN + TẠO ĐƯỜNG + VALIDATION ĐƠN")
    print("=" * 60)
    
    # Khởi tạo detector với validation
    detector = ChannelDetector(
        max_pivots=5,
        max_age_ms=None,
        H=5000,
        point=0.1,
        max_slope=0.0005,           # Giới hạn độ dốc
        min_distance_candles=2,      # 2 điểm phải cách nhau ít nhất 2 nến
        max_penetration_pct=0.3,    # 0.3% phá tối đa
        max_penetrating_candles=2   # Tối đa 2 nến phá
    )
    
    print("\n" + "-" * 60)
    print("TEST 1: Thêm điểm với khoảng cách vừa phải")
    print("-" * 60)
    
    base_timestamp = 1700000000000
    interval_ms = 15 * 60 * 1000  # 15 phút
    
    # Test với điểm cách nhau đủ xa
    test_pivots_ok = [
        {'timestamp': base_timestamp, 'price': 95000, 'type': 'trough'},
        {'timestamp': base_timestamp + interval_ms * 1, 'price': 96000, 'type': 'peak'},
        {'timestamp': base_timestamp + interval_ms * 3, 'price': 94500, 'type': 'trough'},  # Cách 3 nến
        {'timestamp': base_timestamp + interval_ms * 4, 'price': 96500, 'type': 'peak'},
    ]
    
    for i, pivot in enumerate(test_pivots_ok):
        print(f"\n--- Thêm điểm thứ {i+1} ---")
        # Wrap pivot theo format mới của zigzag
        zigzag_output = {
            'candle': {'timestamp': pivot['timestamp'], 'open': 0, 'high': 0, 'low': 0, 'close': 0, 'volume': 0},
            'pivot': pivot
        }
        detector.add_pivot(zigzag_output)
    
    print("\n" + "-" * 60)
    print("Trạng thái sau test 1:")
    state = detector.get_state()
    print(f"  Số đỉnh: {state['num_peaks']}")
    print(f"  Số đáy: {state['num_troughs']}")
    print(f"  Số đường trên hợp lệ: {state['num_upper_lines']}")
    print(f"  Số đường dưới hợp lệ: {state['num_lower_lines']}")
    
    print("\n" + "-" * 60)
    print("TEST 2: Thêm điểm quá gần (sẽ bị loại)")
    print("-" * 60)
    
    detector.reset()
    
    # Test với điểm quá gần nhau
    test_pivots_too_close = [
        {'timestamp': base_timestamp, 'price': 95000, 'type': 'trough'},
        {'timestamp': base_timestamp + interval_ms * 1, 'price': 96000, 'type': 'peak'},
        {'timestamp': base_timestamp + interval_ms * 2, 'price': 94800, 'type': 'trough'},  # Cách 2 nến OK
        {'timestamp': base_timestamp + interval_ms * 2.5, 'price': 94700, 'type': 'trough'},  # Cách 0.5 nến - Quá gần!
    ]
    
    for i, pivot in enumerate(test_pivots_too_close):
        print(f"\n--- Thêm điểm thứ {i+1} ---")
        # Wrap pivot theo format mới của zigzag
        zigzag_output = {
            'candle': {'timestamp': pivot['timestamp'], 'open': 0, 'high': 0, 'low': 0, 'close': 0, 'volume': 0},
            'pivot': pivot
        }
        detector.add_pivot(zigzag_output)
    
    print("\n" + "-" * 60)
    print("Trạng thái sau test 2:")
    state = detector.get_state()
    print(f"  Số đáy: {state['num_troughs']}")
    print(f"  Số đường dưới hợp lệ: {state['num_lower_lines']}")
    print(f"  (Đường tạo từ 2 điểm quá gần đã bị loại)")
    
    print("\n" + "-" * 60)
    print("TEST 3: Thêm điểm tạo đường quá dốc (sẽ bị loại)")
    print("-" * 60)
    
    detector.reset()
    
    # Test với đường quá dốc
    test_pivots_steep = [
        {'timestamp': base_timestamp, 'price': 95000, 'type': 'trough'},
        {'timestamp': base_timestamp + interval_ms * 1, 'price': 96000, 'type': 'peak'},
        {'timestamp': base_timestamp + interval_ms * 3, 'price': 94000, 'type': 'trough'},
        {'timestamp': base_timestamp + interval_ms * 4, 'price': 99000, 'type': 'peak'},  # Tăng 3000 trong 1 nến - Quá dốc!
    ]
    
    for i, pivot in enumerate(test_pivots_steep):
        print(f"\n--- Thêm điểm thứ {i+1} ---")
        # Wrap pivot theo format mới của zigzag
        zigzag_output = {
            'candle': {'timestamp': pivot['timestamp'], 'open': 0, 'high': 0, 'low': 0, 'close': 0, 'volume': 0},
            'pivot': pivot
        }
        detector.add_pivot(zigzag_output)
    
    print("\n" + "-" * 60)
    print("Trạng thái sau test 3:")
    state = detector.get_state()
    print(f"  Số đỉnh: {state['num_peaks']}")
    print(f"  Số đường trên hợp lệ: {state['num_upper_lines']}")
    
    if len(detector.upper_lines) > 0:
        print(f"\n  Chi tiết đường trên còn lại:")
        for line in detector.upper_lines:
            print(f"    - {line['id']}: độ dốc={line['slope']:.8f}")
    
    print("\n" + "-" * 60)
    print("TEST 4: Kiểm tra hàm validate trực tiếp")
    print("-" * 60)
    
    # Tạo đường test thủ công
    point1 = {'id': 0, 'timestamp': base_timestamp, 'price': 95000, 'type': 'peak'}
    point2 = {'id': 1, 'timestamp': base_timestamp + interval_ms * 5, 'price': 96000, 'type': 'peak'}
    
    line_test = detector.create_line(point1, point2)
    is_valid, reason = detector.validate_single_line(line_test)
    
    print(f"  Đường test: {line_test['id']}")
    print(f"  Độ dốc: {line_test['slope']:.8f}")
    print(f"  Khoảng cách: ~5 nến")
    print(f"  Kết quả: {'✓ HỢP LẼ' if is_valid else '✗ KHÔNG HỢP LỆ'}")
    print(f"  Lý do: {reason}")
    
    print("\n" + "=" * 60)
    print("✓ PHẦN 1+2+3 HOÀN THÀNH - Code chạy OK!")
    print("=" * 60)