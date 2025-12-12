"""
========================================
MODULE: ZIGZAG + CHANNEL SLIDING WINDOW
========================================

MỤC ĐÍCH:
---------
Mô phỏng quá trình tìm kênh theo thời gian thực với sliding window.
Vẽ TẤT CẢ các đường từng được tạo ra trong quá trình (kể cả đường cũ).

CÁCH HOẠT ĐỘNG:
---------------
1. Chạy ZigZag qua toàn bộ dữ liệu → tìm tất cả pivot
2. Mô phỏng quá trình realtime với sliding window giới hạn N điểm
3. Mỗi khi có pivot mới:
   - Thêm vào window
   - Tạo đường nối với TẤT CẢ điểm khác trong window
   - Lưu đường vào danh sách tổng
   - Xóa điểm cũ nhất nếu vượt quá giới hạn
4. Kết quả: VẼ TẤT CẢ đường đã từng tạo ra

VÍ DỤ FLOW (max_pivots=3):
-------------------------
Đỉnh 1 → (window: [1])
Đỉnh 2 → (window: [1,2]) → Tạo đường: 1-2
Đỉnh 3 → (window: [1,2,3]) → Tạo đường: 1-3, 2-3
Đỉnh 4 → (window: [2,3,4]) → Tạo đường: 2-4, 3-4  [xóa 1]
Đỉnh 5 → (window: [3,4,5]) → Tạo đường: 3-5, 4-5  [xóa 2]

KẾT QUẢ VẼ: 1-2, 1-3, 2-3, 2-4, 3-4, 3-5, 4-5
"""

import pandas as pd
import plotly.graph_objects as go
from datetime import datetime


def load_csv_data(csv_file):
    """Load dữ liệu từ CSV"""
    print(f"\n📂 Load dữ liệu từ: {csv_file}")
    df = pd.read_csv(csv_file)
    print(f"✓ Đã load {len(df)} nến")
    return df


def run_zigzag_full(df, H=1000, point=1.0):
    """
    Chạy ZigZag qua TOÀN BỘ dữ liệu
    
    OUTPUT:
    - List tất cả pivot tìm được
    """
    import new_zigzag
    
    print(f"\n🔍 Chạy ZigZag (H={H}, point={point})...")
    new_zigzag.reset()
    
    pivots = []
    
    for idx, row in df.iterrows():
        candle = {
            'timestamp': row['timestamp'],
            'open': row['open'],
            'high': row['high'],
            'low': row['low'],
            'close': row['close'],
            'volume': row['volume']
        }
        
        result = new_zigzag.process_new_candle(candle, H=H, point=point)
        if result and result['pivot']:
            pivots.append(result['pivot'])
    
    peaks = [p for p in pivots if p['type'] == 'peak']
    troughs = [p for p in pivots if p['type'] == 'trough']
    
    print(f"✓ Tìm được {len(pivots)} pivot:")
    print(f"  - {len(peaks)} đỉnh")
    print(f"  - {len(troughs)} đáy")
    
    return pivots


def simulate_sliding_window(df, pivots, max_pivots=3, H=1000, point=1.0):
    """
    Mô phỏng sliding window và tạo TẤT CẢ đường
    
    INPUT:
    - df: DataFrame chứa dữ liệu nến (để feed vào detector)
    - pivots: List tất cả pivot từ ZigZag
    - max_pivots: Giới hạn sliding window
    - H, point: Tham số để validate
    
    OUTPUT:
    - all_upper_lines: List TẤT CẢ đường đỉnh-đỉnh
    - all_lower_lines: List TẤT CẢ đường đáy-đáy
    """
    import channel_detector
    
    print(f"\n📐 Mô phỏng sliding window (max={max_pivots} pivot)...")
    
    # Khởi tạo detector
    detector = channel_detector.ChannelDetector(
        max_pivots=max_pivots,
        max_age_ms=None,
        H=H,
        point=point,
        max_slope=0.00003,
        min_distance_candles=1,
        max_penetration_pct=0.3,      # 0.3% phá tối đa
        max_penetrating_candles=3,    # Tối đa 2 nến phá
        max_convergence=0.00002,    # Cho phép hội tụ thoáng
        max_divergence=0.000015     # Cho phép phân kỳ ít
    )
    
    # Sliding windows
    peaks_window = []
    troughs_window = []
    
    # Feed tất cả candles vào detector trước
    print(f"  → Feed {len(df)} nến vào detector...")
    for idx, row in df.iterrows():
        candle = {
            'timestamp': row['timestamp'],
            'open': row['open'],
            'high': row['high'],
            'low': row['low'],
            'close': row['close'],
            'volume': row['volume']
        }
        detector.candles_list.append(candle)
    
    # Lưu TẤT CẢ đường đã tạo
    all_upper_lines = []
    all_lower_lines = []
    
    # Set để tránh tạo trùng đường
    created_peak_pairs = set()
    created_trough_pairs = set()
    
    # Hàm chuyển timestamp
    def to_ms(ts):
        if isinstance(ts, (int, float)):
            return int(ts)
        try:
            return int(pd.to_datetime(ts).timestamp() * 1000)
        except:
            return 0
    
    # Hàm tạo đường từ 2 điểm
    def create_line_from_pivots(p1, p2, p1_idx, p2_idx):
        point1 = {
            'id': p1_idx,
            'timestamp': to_ms(p1['timestamp']),
            'price': p1['price'],
            'type': p1['type']
        }
        point2 = {
            'id': p2_idx,
            'timestamp': to_ms(p2['timestamp']),
            'price': p2['price'],
            'type': p2['type']
        }
        
        line = detector.create_line(point1, point2)
        is_valid, reason = detector.validate_single_line(line)
        
        line['is_valid'] = is_valid
        line['reason'] = reason
        line['timestamp1_str'] = p1['timestamp']
        line['timestamp2_str'] = p2['timestamp']
        line['pivot1_idx'] = p1_idx
        line['pivot2_idx'] = p2_idx
        
        return line
    
    # Xử lý từng pivot theo thứ tự
    pivot_counter = 0
    
    for pivot in pivots:
        pivot_type = pivot['type']
        
        if pivot_type == 'peak':
            # ===== XỬ LÝ ĐỈNH =====
            
            print(f"\n  Đỉnh #{pivot_counter}: Price={pivot['price']:.2f}")
            print(f"    Window trước: {[idx for idx, _ in peaks_window]}")
            
            # XÓA ĐIỂM CŨ TRƯỚC nếu window đã đầy
            if len(peaks_window) >= max_pivots:
                removed_idx, removed_pivot = peaks_window.pop(0)
                print(f"    ✗ Xóa đỉnh #{removed_idx} (window đầy)")
            
            # THÊM ĐIỂM MỚI vào window
            peaks_window.append((pivot_counter, pivot))
            print(f"    Window sau: {[idx for idx, _ in peaks_window]}")
            
            # TẠO ĐƯỜNG với TẤT CẢ đỉnh khác trong window hiện tại
            current_idx = len(peaks_window) - 1
            
            for i in range(len(peaks_window) - 1):
                idx1, p1 = peaks_window[i]
                idx2, p2 = peaks_window[current_idx]
                
                # Kiểm tra đã tạo chưa
                pair = tuple(sorted([idx1, idx2]))
                if pair in created_peak_pairs:
                    continue
                
                created_peak_pairs.add(pair)
                
                # Tạo đường
                line = create_line_from_pivots(p1, p2, idx1, idx2)
                all_upper_lines.append(line)
                
                status = "✓" if line['is_valid'] else "✗"
                reason_msg = f" ({line['reason']})" if not line['is_valid'] else ""
                print(f"    → Tạo đường {idx1}-{idx2} {status}{reason_msg}")
        
        else:
            # ===== XỬ LÝ ĐÁY =====
            
            print(f"\n  Đáy #{pivot_counter}: Price={pivot['price']:.2f}")
            print(f"    Window trước: {[idx for idx, _ in troughs_window]}")
            
            # XÓA ĐIỂM CŨ TRƯỚC nếu window đã đầy
            if len(troughs_window) >= max_pivots:
                removed_idx, removed_pivot = troughs_window.pop(0)
                print(f"    ✗ Xóa đáy #{removed_idx} (window đầy)")
            
            # THÊM ĐIỂM MỚI vào window
            troughs_window.append((pivot_counter, pivot))
            print(f"    Window sau: {[idx for idx, _ in troughs_window]}")
            
            # TẠO ĐƯỜNG với TẤT CẢ đáy khác trong window hiện tại
            current_idx = len(troughs_window) - 1
            
            for i in range(len(troughs_window) - 1):
                idx1, t1 = troughs_window[i]
                idx2, t2 = troughs_window[current_idx]
                
                # Kiểm tra đã tạo chưa
                pair = tuple(sorted([idx1, idx2]))
                if pair in created_trough_pairs:
                    continue
                
                created_trough_pairs.add(pair)
                
                # Tạo đường
                line = create_line_from_pivots(t1, t2, idx1, idx2)
                all_lower_lines.append(line)
                
                status = "✓" if line['is_valid'] else "✗"
                reason_msg = f" ({line['reason']})" if not line['is_valid'] else ""
                print(f"    → Tạo đường {idx1}-{idx2} {status}{reason_msg}")
        
        pivot_counter += 1
    
    print(f"\n✓ Hoàn thành mô phỏng!")
    print(f"  - Tổng đường đỉnh: {len(all_upper_lines)}")
    print(f"    + Hợp lệ: {len([l for l in all_upper_lines if l['is_valid']])}")
    print(f"    + Không hợp lệ: {len([l for l in all_upper_lines if not l['is_valid']])}")
    print(f"  - Tổng đường đáy: {len(all_lower_lines)}")
    print(f"    + Hợp lệ: {len([l for l in all_lower_lines if l['is_valid']])}")
    print(f"    + Không hợp lệ: {len([l for l in all_lower_lines if not l['is_valid']])}")
    
    return all_upper_lines, all_lower_lines


def create_chart(df, pivots, upper_lines, lower_lines, title="ZigZag + Channel (Sliding Window)"):
    """
    Tạo biểu đồ với TẤT CẢ đường
    """
    print(f"\n🎨 Tạo biểu đồ...")
    
    fig = go.Figure()
    
    # 1. Vẽ nến
    fig.add_trace(go.Candlestick(
        x=df['timestamp'],
        open=df['open'],
        high=df['high'],
        low=df['low'],
        close=df['close'],
        name='Price',
        increasing_line_color='#26a69a',
        increasing_fillcolor='#26a69a',
        decreasing_line_color='#ef5350',
        decreasing_fillcolor='#ef5350',
        showlegend=False
    ))
    
    # 2. Đánh dấu pivot
    peaks = [p for p in pivots if p['type'] == 'peak']
    troughs = [p for p in pivots if p['type'] == 'trough']
    
    if len(peaks) > 0:
        fig.add_trace(go.Scatter(
            x=[p['timestamp'] for p in peaks],
            y=[p['price'] for p in peaks],
            mode='markers',
            name=f'Peaks ({len(peaks)})',
            marker=dict(symbol='triangle-down', size=10, color='#ff4444',
                       line=dict(color='white', width=1)),
            showlegend=True
        ))
    
    if len(troughs) > 0:
        fig.add_trace(go.Scatter(
            x=[t['timestamp'] for t in troughs],
            y=[t['price'] for t in troughs],
            mode='markers',
            name=f'Troughs ({len(troughs)})',
            marker=dict(symbol='triangle-up', size=10, color='#44ff44',
                       line=dict(color='white', width=1)),
            showlegend=True
        ))
    
    # 3. Vẽ đường đỉnh-đỉnh
    for line in upper_lines:
        x_coords = [line['timestamp1_str'], line['timestamp2_str']]
        y_coords = [line['point1']['price'], line['point2']['price']]
        
        if line['is_valid']:
            line_style = dict(color='#ff6b6b', width=2, dash='solid')
            opacity = 0.7
        else:
            line_style = dict(color='#ff9999', width=1.5, dash='dash')
            opacity = 0.4
        
        fig.add_trace(go.Scatter(
            x=x_coords,
            y=y_coords,
            mode='lines',
            line=line_style,
            opacity=opacity,
            name=f"Upper {line['pivot1_idx']}-{line['pivot2_idx']}",
            showlegend=False,
            hovertemplate=f"<b>Upper Line {line['pivot1_idx']}-{line['pivot2_idx']}</b><br>" +
                         f"Valid: {line['is_valid']}<br>" +
                         f"Slope: {line['slope']:.8f}<extra></extra>"
        ))
    
    # 4. Vẽ đường đáy-đáy
    for line in lower_lines:
        x_coords = [line['timestamp1_str'], line['timestamp2_str']]
        y_coords = [line['point1']['price'], line['point2']['price']]
        
        if line['is_valid']:
            line_style = dict(color='#6bff6b', width=2, dash='solid')
            opacity = 0.7
        else:
            line_style = dict(color='#99ff99', width=1.5, dash='dash')
            opacity = 0.4
        
        fig.add_trace(go.Scatter(
            x=x_coords,
            y=y_coords,
            mode='lines',
            line=line_style,
            opacity=opacity,
            name=f"Lower {line['pivot1_idx']}-{line['pivot2_idx']}",
            showlegend=False,
            hovertemplate=f"<b>Lower Line {line['pivot1_idx']}-{line['pivot2_idx']}</b><br>" +
                         f"Valid: {line['is_valid']}<br>" +
                         f"Slope: {line['slope']:.8f}<extra></extra>"
        ))
    
    # 5. Layout
    fig.update_layout(
        title={'text': title, 'x': 0.5, 'xanchor': 'center',
               'font': {'size': 18, 'color': '#d1d4dc'}},
        paper_bgcolor='#0d0e12',
        plot_bgcolor='#0d0e12',
        font={'color': '#d1d4dc'},
        xaxis=dict(title='Time', gridcolor='#1e222d', linecolor='#2b2f3a',
                  rangeslider=dict(visible=False)),
        yaxis=dict(title='Price', gridcolor='#1e222d', linecolor='#2b2f3a',
                  side='right'),
        height=800,
        legend=dict(x=0.01, y=0.99, bgcolor='rgba(13,14,18,0.9)',
                   bordercolor='#2b2f3a', borderwidth=1),
        hovermode='closest'
    )
    
    print("✓ Đã tạo biểu đồ!")
    return fig


def show_chart(fig):
    """Hiển thị biểu đồ trong browser"""
    import tempfile
    import webbrowser
    
    config = {'scrollZoom': True, 'displayModeBar': True,
             'displaylogo': False, 'responsive': True}
    
    html = fig.to_html(config=config, include_plotlyjs='cdn')
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.html', 
                                    delete=False, encoding='utf-8') as f:
        f.write(html)
        temp_path = f.name
    
    webbrowser.open('file://' + temp_path)
    print(f"✓ Đã mở biểu đồ: {temp_path}")


def run_analysis(csv_file="data/BTCUSDT_15m.csv", max_pivots=3, H=1000, point=1.0):
    """
    Chạy phân tích hoàn chỉnh
    
    INPUT:
    - csv_file: File dữ liệu
    - max_pivots: Giới hạn sliding window
    - H, point: Tham số ZigZag
    
    OUTPUT:
    - df, pivots, upper_lines, lower_lines, fig
    """
    print("="*70)
    print("🚀 ZIGZAG + CHANNEL SLIDING WINDOW ANALYSIS")
    print("="*70)
    print(f"⚙️  Tham số:")
    print(f"   - Sliding window: {max_pivots} pivot")
    print(f"   - ZigZag: H={H}, point={point}")
    print("="*70)
    
    # 1. Load data
    df = load_csv_data(csv_file)
    
    # 2. Chạy ZigZag qua toàn bộ
    pivots = run_zigzag_full(df, H=H, point=point)
    
    if len(pivots) < 2:
        print("\n⚠️  Không đủ pivot!")
        return None
    
    # 3. Mô phỏng sliding window
    upper_lines, lower_lines = simulate_sliding_window(df, pivots, max_pivots, H, point)
    
    # 4. Vẽ biểu đồ
    title = f"ZigZag + Channel (Window={max_pivots}, H={H})"
    fig = create_chart(df, pivots, upper_lines, lower_lines, title)
    
    # 5. Hiển thị
    show_chart(fig)
    
    # Tổng kết
    print("\n" + "="*70)
    print("✅ HOÀN THÀNH!")
    print("="*70)
    print(f"📊 Dữ liệu: {len(df)} nến")
    print(f"📍 Pivot: {len(pivots)} điểm")
    print(f"📐 Đường đỉnh: {len(upper_lines)} ({len([l for l in upper_lines if l['is_valid']])} hợp lệ)")
    print(f"📐 Đường đáy: {len(lower_lines)} ({len([l for l in lower_lines if l['is_valid']])} hợp lệ)")
    print("\n💡 Giải thích:")
    print(f"   - Sliding window giới hạn {max_pivots} pivot gần nhất")
    print(f"   - Vẽ TẤT CẢ đường từng được tạo ra (kể cả đường cũ)")
    print(f"   - Nét liền = hợp lệ, Nét đứt = không hợp lệ")
    print("="*70)
    
    return df, pivots, upper_lines, lower_lines, fig


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    
    print("\n" + "🎯 "*25)
    print("TEST SLIDING WINDOW")
    print("🎯 "*25)
    
    # Chạy với sliding window = 3 pivot
    run_analysis(
        csv_file="data/BTCUSDT_15m.csv",
        max_pivots=3,
        H=1000,
        point=1.0
    )
    
    print("\n✅ Test hoàn tất!")