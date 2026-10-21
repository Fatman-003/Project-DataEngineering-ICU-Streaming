# Quy tắc cảnh báo và quản lý False Alarm

## 1. Mục tiêu

Cảnh báo `SUSTAINED_HYPOTENSION` theo dõi một episode MAP thấp kéo dài.
Hệ thống chấp nhận những lần MAP tạm trở lại vùng bình thường trong thời gian
ngắn, nhưng không tính khoảng hồi phục đó là thời gian MAP thấp.

Đây là cảnh báo huyết động sớm dựa trên MAP cho dự án. Nó không phải chẩn đoán
sepsis. Recovery debounce 30 giây là **quyết định kỹ thuật của dự án**, nhằm
giảm reset/flapping do dao động ngắn; chưa phải ngưỡng được xác nhận lâm sàng.

## 2. Các ngưỡng chính

Nguồn cấu hình là [runtime_config.py](../jobs/flink/runtime_config.py).

| Ngưỡng | Giá trị | Dùng để làm gì? |
|---|---|---|
| MAP thấp | <65 mmHg | Bắt đầu hoặc tiếp tục low evidence. |
| Hypotension confirmation | 300 giây event-time | Tổng thời gian MAP thấp cần có trong một episode trước khi cảnh báo. |
| Maximum MAP sample gap | ≤90 giây | Khoảng cách event-time tối đa giữa hai MAP được chấp nhận liên tiếp. |
| Recovery threshold | ≥65 mmHg | Bắt đầu `RECOVERY_CANDIDATE`. |
| Recovery confirmation | 30 giây event-time | Xác nhận recovery, kết thúc episode và re-arm. |
| Out-of-order/lateness tolerance | 30 giây | Tạo bounded watermark để xác định event còn đủ mới cho realtime. |

Hai giá trị 30 giây có vai trò riêng: một giá trị xác nhận recovery, giá trị
kia dùng cho thứ tự/freshness của stream. Chúng không thay thế gap 90 giây
hoặc thời gian xác nhận 300 giây. Event tại hoặc trước watermark hiệu lực
lúc đến là `LATE_EVENT`; đây không phải phép trừ đồng hồ thực với `event_time`.

## 3. Hypotension episode hoạt động như thế nào?

MAP <65 bắt đầu một episode hoặc tiếp tục phần MAP thấp đang quan sát.
Khi một MAP thấp được chấp nhận chứng minh tổng low evidence đã đủ 300 giây,
Flink phát một `SUSTAINED_HYPOTENSION`. Các MAP thấp sau đó trong cùng episode
không tạo thêm alert.

Episode có thể gồm nhiều đoạn thấp, ngăn bởi transient recovery. Vì vậy
không mô tả rule là “MAP tuyệt đối thấp không có bất kỳ mẫu normal nào trong
năm phút”. `episode_start` vẫn là lúc episode bắt đầu; `trigger_time` là mẫu
thấp xác nhận đủ low evidence. Hai mốc này có thể cách nhau hơn 300 giây.

## 4. Recovery không reset ngay lập tức

Khi MAP trở lại ≥65, hệ thống chuyển sang `RECOVERY_CANDIDATE`. Nếu MAP thấp
lại trước 30 giây, candidate bị hủy; episode và low evidence trước đó được
giữ. Nếu khoảng recovery đạt ít nhất 30 giây, episode kết thúc, low evidence
được xóa và lần MAP thấp tiếp theo bắt đầu episode mới.

Thời gian được kiểm chứng khi có MAP tiếp theo được chấp nhận, với gap không
quá 90 giây. Ví dụ normal tại T+150, normal tại T+179 vẫn là candidate; normal
tại T+180 xác nhận recovery. Nếu MAP thấp trở lại ngay T+180, khoảng normal
đã kéo dài đúng 30 giây cũng kết thúc episode cũ, rồi mẫu thấp đó mở episode
mới. Nếu thấp lại T+179 thì chỉ có 29 giây recovery: episode được giữ.

Watermark tiến lên mà không có MAP tiếp theo không tự tạo bằng chứng recovery.

## 5. Tại sao không dùng số lượng sample?

Thời gian 30 giây có ý nghĩa ổn định hơn một con số như “ba mẫu normal”.
Cadence demo thường gần một MAP mỗi bệnh nhân mỗi sáu giây, nên 30 giây ứng
với vài mẫu MAP. Khi cadence thay đổi, nguồn quyết định vẫn là event-time,
không phải số mẫu.

## 6. Không tính transient recovery vào low duration

Ví dụ, với các MAP hợp lệ cách nhau không quá 90 giây:

```text
T+0   → T+150: 150 giây low
T+150 → T+170:  20 giây normal / recovery candidate
T+170 → T+320: 150 giây low
```

Candidate 20 giây không reset toàn bộ episode và cũng không được cộng vào
low evidence. Tại T+300 chỉ có 280 giây thấp; tại T+320 mới có 300 giây thấp
và có thể phát alert. Chênh lệch giữa các quan sát được tính theo trạng thái
MAP đứng trước khoảng đó: mẫu normal đóng đoạn thấp; mẫu thấp trở lại đóng
khoảng recovery. Không tính thêm thời gian khi không có quan sát chứng minh.

## 7. State machine đơn giản

```text
NORMAL
   |
   | MAP <65
   v
LOW_EPISODE
   |
   | MAP ≥65
   v
RECOVERY_CANDIDATE
   |
   +----------------------------------+
   |                                  |
   | thấp lại trước 30s               | recovery đạt ≥30s
   v                                  v
LOW_EPISODE                       RECOVERED
giữ evidence, bỏ thời gian normal  xóa evidence, re-arm
                                      |
                                      | MAP <65
                                      v
                                 LOW_EPISODE mới
```

Ở mọi trạng thái, gap MAP >90 giây làm mất tính liên tục của quan sát. Mẫu
thấp sau gap bắt đầu episode mới; recovery debounce không che lấp mất dữ liệu.

## 8. Các cơ chế giảm false alarm

| Cơ chế | Tác dụng |
|---|---|
| Schema/value validation | Dữ liệu sai cấu trúc hoặc ngoài giới hạn kỹ thuật không tham gia rule. |
| Deduplication | Cùng `event_id` không được cộng thêm evidence hoặc tạo alert lặp. |
| Late-event control | Dữ liệu quá cũ đi vào DLQ, không sửa ngược cảnh báo realtime. |
| Gap MAP ≤90 giây | Không coi một khoảng dài thiếu MAP là quan sát liên tục. |
| Low evidence ≥300 giây | Một đợt MAP thấp ngắn không đủ điều kiện cảnh báo. |
| Recovery confirmation 30 giây | Một vài mẫu normal ngắn không reset cả episode. |
| One alert per episode | MAP tiếp tục thấp không phát cảnh báo lặp liên tục. |
| Confirmed recovery re-arm | Một episode mới đủ evidence vẫn có thể phát alert mới. |

Accepted out-of-order MAP được xét lại theo event-time trong phần lịch sử
chưa chốt. Ví dụ MAP thấp ở 02:24 đến trước MAP normal ở 02:18: nếu normal vẫn
được chấp nhận, sáu giây recovery đó phải bị loại khỏi low evidence, thay vì
bị bỏ qua do thứ tự nhận. Recovery cùng timestamp được ưu tiên hơn low MAP.

## 9. Ví dụ tình huống

| Tình huống | Kết quả |
|---|---|
| MAP thấp đủ 300 giây, gap hợp lệ | Một alert. |
| MAP thấp 150 giây rồi recovery đủ 30 giây | Không alert; evidence cũ được xóa. |
| 150 giây low, 20 giây normal, 150 giây low | Giữ episode; alert ở khoảng T+320, không phải T+300. |
| MAP normal đủ 30 giây rồi thấp lại | Episode mới phải tự tích đủ 300 giây thấp. |
| `64 → 66 → 64 → 66 → 63 → 67 → 64`, mỗi đoạn normal dưới 30 giây | Không reset/re-arm ở mỗi lần vượt ngưỡng; chỉ cộng các đoạn thấp. |
| Gap MAP 91 giây | Continuity bị phá vỡ, không mang evidence thấp cũ qua gap. |
| Duplicate / invalid / too-late MAP | Không thêm low evidence hoặc recovery evidence; theo chính sách DLQ hiện có. |

Sau một alert, transient recovery vẫn thuộc episode đó và không cho phép
alert thứ hai. Trong chuỗi quan sát không bị gap, recovery được xác nhận mới
kết thúc episode đó; episode mới đủ evidence được phép phát alert tiếp theo.
Gap >90 giây là mất continuity quan sát, không phải bằng chứng patient recovery.

## 10. Giới hạn của rule hiện tại

Chỉ MAP được chấp nhận điều khiển low/recovery state. HR, RR, SpO2, SBP, DBP
hoặc Lactate bình thường không reset hay xác nhận recovery cho rule này.
Hệ thống chưa kết hợp nhiều tín hiệu thành một sepsis score.

Alert đã phát không bị thu hồi khi dữ liệu lịch sử đến sau. Một event quá
watermark đi vào `icu.v2.deadletter` và không sửa ngược alert đã lưu. Với event
out-of-order còn được chấp nhận, trạng thái hiện tại được xét lại nhưng alert
đã gửi vẫn bất biến. Đây là lựa chọn realtime có giới hạn freshness, không
phải tái dựng lâm sàng hoàn hảo từ dữ liệu đến trễ tùy ý.

## 11. Luồng kỹ thuật

```text
cleaned MAP
→ Flink managed state theo stay_id
→ episode + low evidence + recovery debounce
→ xác nhận alert từ mẫu MAP thấp
→ icu.v2.alerts
   ├→ Realtime Gateway → WebSocket
   └→ Storage Writer → alerts
```

Candidate chỉ nằm trong processing state, không tạo Kafka topic hay bảng DB
riêng. Nhánh alert không chờ watermark thêm 30 hoặc 90 giây sau khi đã đủ
low evidence; mục tiêu Flink → WebSocket vẫn dưới một giây, giới hạn chấp nhận
dưới hai giây. FastVerify dùng event-time tăng tốc, nên recovery 30 giây không
buộc phải chờ 30 giây thực.

Xem [KAFKA_FLINK_FLOW.md](../KAFKA_FLINK_FLOW.md) cho toàn bộ pipeline và
[DATABASE_DESIGN.md](DATABASE_DESIGN.md) cho cách lưu final alert.
