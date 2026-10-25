# Quy tắc cảnh báo MAP và quản lý False Alarm

`HYPOTENSION_SUSTAINED` là cảnh báo sớm khi một episode có đủ MAP thấp.
Đây **không phải chẩn đoán sepsis**. Ngưỡng và cơ chế lọc dưới đây là quyết
định kỹ thuật của dự án, chưa được trình bày như quy tắc đã xác nhận lâm sàng.
Chỉ MAP hợp lệ tham gia rule; HR/RR/SBP/DBP/SpO2/Lactate không xác nhận recovery.

Nguồn cấu hình: [runtime_config.py](../jobs/flink/runtime_config.py).
Nguồn xử lý: [hypotension.py](../jobs/flink/hypotension.py).
Các biên được kiểm tra trong [test_alarm_quality.py](../tests/test_alarm_quality.py).

## Các khoảng thời gian có vai trò khác nhau

| Cấu hình | Giá trị hiện tại | Vai trò |
|---|---|---|
| MAP thấp | `<65 mmHg` | Low observation |
| MAP normal/positive | `>=65 mmHg` | Positive recovery observation |
| Low evidence | `>=30 giây` Event Time | Đủ điều kiện cảnh báo, xét ở bucket low |
| MAP bucket | `1 giây` UTC | Tối đa một observation quyết định/bucket |
| Recovery | `5` positive bucket trong `(t−10s, t]` | Xác nhận recovery, xóa evidence và re-arm |
| Out-of-order tolerance | `2 giây` | Tạo bounded Watermark |
| Maximum MAP gap | `3 giây`, gồm biên bằng 3 | Giữ continuity của episode |
| Statistical window | `5 phút`, slide `1 phút` | Mean telemetry, không phải thời gian xác nhận alert |

## Chuẩn hóa Normal và HF thành observation 1 giây

MAP đã qua parse/validation/dedup/Event Time được nhóm theo stay và bucket
UTC `[s, s+1 giây)`. Representative của bucket là **median** các giá trị MAP
được giữ lại. Với số mẫu chẵn, median là trung bình hai giá trị giữa; ví dụ
64,9 và 65,1 có median 65, được xem là normal. Không làm tròn số về integer
trước khi so ngưỡng.

Mỗi bucket giữ tối đa **256 giá trị thực** bằng deterministic bottom-k theo
hash `event_id`. Normal/HF100/HF200 có tối đa 1/100/200 MAP mỗi giây cho một
stay trong workload chuẩn, nên giữ đủ và median là chính xác. Khi một bucket
có hơn 256 MAP hợp lệ, median dùng tập mẫu giữ lại, không phải median chính
xác của toàn bộ đầu vào. Thứ tự đến không làm thay đổi lựa chọn bottom-k.
Giới hạn này chỉ áp dụng rule MAP; không làm giảm số RAW hay wide telemetry.

Một bucket có median `>=65` chỉ thêm **một** positive observation. Vì vậy
5 RAW sample normal trong 25 ms của HF200 không thể tự xác nhận recovery.
Năm bucket normal ở năm giây khác nhau có cùng ý nghĩa trong Normal và HF.

Bucket chỉ được xử lý khi Watermark đi qua cuối bucket; phần đã chốt được
gộp vào episode state rồi bỏ khỏi pending state. Watermark tiến mà không có
MAP mới không tự sinh observation hoặc tự cộng low/recovery evidence.

## Low evidence và một alert mỗi episode

Bucket low đầu tiên mở episode với evidence bằng 0. Mỗi observation tiếp
theo hợp lệ cộng chênh lệch thời gian giữa hai bucket **nếu bucket trước là
low**, với điều kiện continuity còn hợp lệ. Bucket trước normal thì không
cộng khoảng đó. Ví dụ chỉ có low ở T+0 chưa chứng minh 30 giây; low mỗi giây
từ T+0 đến T+29 cho 29 giây, low tại T+30 mới chứng minh 30 giây.

Flink chỉ xét phát khi bucket hiện tại cũng là low, evidence ít nhất 30 giây
và episode chưa alert. Bucket normal có thể đóng một khoảng low nhưng không
tự phát alert. Khi đủ điều kiện, phát một `SEPSIS_ALERT_CREATED` vào
`icu.alerts`: `alert_type=HYPOTENSION_SUSTAINED`, `severity=CRITICAL`,
`status=ACTIVE`. `alert_time` là đầu bucket kích hoạt, `trigger_value` là
median bucket đó. Các bucket low sau trong cùng episode không phát lặp.

`alert_id` là UUIDv5 theo stay, loại alert và thời điểm mở episode; retry cùng
episode giữ identity. Episode start chỉ ở processing state và tham gia ID;
body/table không có cột `episode_start`.

## Multi-Vital Context for Hypotension Alerts

MAP vẫn là trigger duy nhất: `<65 mmHg`, đủ 30 giây low evidence theo rule
episode hiện có. HR/RR/SBP/SpO2 chỉ bổ sung ngữ cảnh, không tạo alert độc lập,
không xác nhận recovery, không đổi `severity=CRITICAL` hay identity của alert.

| Tín hiệu | Điều kiện bổ sung | Phân loại |
|---|---|---|
| HR | `>100 bpm` | `HIGH_HR` |
| RR | `>=22 breaths/min` | `HIGH_RR` |
| SBP | `<=100 mmHg` | `LOW_SBP` |
| SpO2 | `<=91%` | `LOW_SPO2` |

Mỗi tín hiệu dùng median trong **cùng bucket Event Time 1 giây** kích hoạt
MAP alert, sau validation/dedup/Late Event filtering. Bucket được chốt bằng
Watermark hiện có; không đợi thêm tín hiệu còn thiếu và không dùng lại giá trị
từ bucket trước. Mỗi tín hiệu giữ tối đa 256 mẫu theo cùng deterministic
bottom-k như MAP, đủ cho Normal/HF100/HF200; state bucket được xóa khi chốt.
Có giá trị nhưng không thỏa điều kiện thì là `NORMAL`; không có quan sát hợp
lệ trong bucket đó thì là `UNKNOWN`, không được coi là normal và không ngăn
MAP alert. `additional_abnormalities` chỉ đếm HR/RR/SBP/SpO2 bất thường, không
đếm MAP hoặc `UNKNOWN`; đây không phải điểm nguy cơ y khoa.

Các giá trị/phân loại được ghi trong **`trigger_message`** (tối đa 1024 ký tự),
không thêm trường JSON. Ví dụ:

```text
MAP 62 mmHg (< 65): >= 30s accumulated low evidence; HR=112 HIGH_HR; RR=25 HIGH_RR; SBP=95 LOW_SBP; SpO2=97 NORMAL; additional_abnormalities=3
```

Các ngưỡng này là tham số ngữ cảnh thử nghiệm phục vụ nghiên cứu kỹ thuật,
**không phải chẩn đoán sepsis đã được xác nhận**.

## Transient recovery và confirmed recovery

Một observation normal không reset ngay episode. Các positive timestamp
được giữ trong rolling interval **`(t−10s, t]`**: đúng T−10 bị loại, timestamp
mới hơn T−10 được tính. Đủ 5 positive thì **confirmed recovery**: xóa episode,
xóa low evidence và re-arm. Positive không bắt buộc liên tiếp; xen giữa có
low vẫn có thể recovery nếu tổng 5 positive còn trong cửa sổ và gap hợp lệ.

Chưa đủ 5 là transient recovery: episode/evidence vẫn giữ; khoảng normal
không đóng góp low time. Khi low trở lại, tiếp tục tích lũy phần low mới.
Không có cooldown cố định 15 phút. Low sau confirmed recovery bắt đầu một
episode độc lập, phải tự tích đủ 30 giây mới phát alert mới.

Ví dụ các bucket normal tại T+10,11,12,13 mới là 4 positive; bucket T+14
normal chốt xong thì đủ 5, không cần đợi thêm 10 giây. Ngược lại normal tại
T+10,13,16,19,22 không đủ 5 trong cùng `(t−10,t]`: tại T+22, T+10 đã hết hạn.

## Khoảng normal không cộng vào low duration

Với MAP được quan sát mỗi giây, không bị gap:

```text
T+0  → T+15 : 15 giây low
T+15 → T+18 :  3 giây normal (3 positive, chưa recovery)
T+18 → T+33 : 15 giây low

T+30: low evidence = 27 giây
T+33: low evidence = 30 giây → alert khi bucket T+33 được chốt
```

Episode kéo dài 33 giây, low evidence chỉ 30 giây. Vì vậy rule không yêu cầu
MAP thấp tuyệt đối không gián đoạn suốt 30 giây khi transient recovery được
chấp nhận. Confirmed recovery mới xóa evidence cũ.

## Out-of-order, Late Event và sample gap

| Tình huống | Cách xử lý hiện tại |
|---|---|
| Sai schema/identity/unit/range, số không hữu hạn | DLQ trước MAP operator, không thêm evidence |
| Cùng stream + `event_id` | Dedup TTL 24 giờ processing time; duplicate → DLQ |
| Đến khác thứ tự nhưng timestamp sau Watermark | Chấp nhận, đóng góp median bucket chưa chốt |
| Timestamp tại hoặc trước Watermark hiệu lực khi đến | Late Event → DLQ; không sửa episode đã chốt |
| Khoảng cách MAP đúng 3 giây | Vẫn giữ continuity |
| Gap lớn hơn 3 giây | Xóa continuity/evidence; bucket low tiếp theo mở episode mới |

Watermark là ranh giới của Event Time đang xử lý, không phải phép trừ đồng
hồ thực lúc nhận với timestamp. Config tolerance 2 giây không bảo đảm mọi
message trễ đúng 2 giây thực đều được chấp nhận; Watermark hiệu lực quyết định.

Continuity kiểm tra chênh lệch đầu bucket và khoảng giữa quan sát RAW cuối
bucket trước/đầu bucket sau. Ví dụ gap RAW 3,001 giây vẫn reset dù đầu hai
bucket chỉ cách nhau 3 giây. Mất continuity không phải bằng chứng patient đã
hồi phục; nó chỉ ngăn nối evidence qua đoạn thiếu dữ liệu. Sau gap, một
episode mới vẫn có thể phát alert khi tích lại đủ evidence.

Accepted Out-of-order normal có thể làm median bucket chưa chốt trở thành
normal, giảm low evidence hoặc thêm một positive khi chốt theo thứ tự Event
Time. Nó không chỉnh lại bucket đã chốt. Too-late recovery không thu hồi
alert đã gửi; không có retraction hay luồng correction alert.

RAW duplicate/late dùng `error_code=PROCESSING_ERROR`, `retryable=false`;
`error_message` bắt đầu bằng `DUPLICATE_EVENT:` hoặc `LATE_EVENT:`. Chi tiết
payload/header ở [Kafka contract](KAFKA_MESSAGE_FORMATS.md).

## Độ trễ xác nhận và độ trễ vận chuyển

Quyết định MAP chờ bucket finality. Bucket 1 giây và tolerance 2 giây thường
cần khoảng **3 giây Event Time từ đầu bucket** khi stream tiến đều, cộng độ
trễ xử lý/Watermark propagation. Đây không phải cam kết trần 3 giây thực;
khi input dừng hoặc hệ thống backlog, finality có thể chậm hơn.

Sau đó alert đi Flink → Kafka → Gateway → WebSocket, độc lập DB. FastVerify
đo mốc thực `produced_at` của alert (Gateway thêm alias `alert_emitted_at`),
`gateway_received_at` và `websocket_received_at`; ngưỡng transport check là
dưới 2 giây. Phép đo này **không bao gồm** 30 giây evidence và thời gian chốt
bucket, không được dùng để khẳng định toàn bộ thời gian phát hiện dưới 2 giây.

## Các tình huống để đọc rule

Các mốc là Event Time của bucket; kết quả xuất hiện sau finality.

| Ca | Timeline | Kết quả |
|---|---|---|
| A. Đủ 30 giây low | MAP 62 mỗi giây T+0…30 | T+29 chưa alert; T+30 một alert |
| B. Low ngắn | Low T+0…9, normal T+10…14 | 5 positive xác nhận recovery, không alert |
| C. Bốn normal | Low T+0…9, normal T+10…13, low lại T+14 | Chưa recovery; episode cũ tiếp tục |
| D. Năm normal | Low T+0…9, normal T+10…14 | Chốt T+14 xóa evidence và re-arm |
| E. Dao động ngưỡng | Median lần lượt 64,66,64,66,63,67,64 mỗi giây | Mỗi bucket normal thêm positive; chưa đủ 5 ở đoạn này, chỉ khoảng low được cộng |
| F. Duplicate/invalid/too-late | Lặp normal cùng event_id, MAP −5 hoặc normal trước Watermark | Bị loại, không thêm positive/low evidence |
| G. Episode thứ hai | Low T+0…30; normal T+31…35; low T+36…66 | Hai alert T+30/T+66, hai `alert_id` khác nhau |
| H. Normal so với HF | 1/100/200 MAP normal trong cùng một bucket | Đều tối đa 1 positive; 5 bucket qualified mới có thể recovery |

Trong simulator mặc định, P02 low từ T+12 nên đủ evidence T+42. P03 mỗi chu
kỳ 60 giây low ở 0…14 và 18…28; ba normal đầu chưa recovery, nhưng phần
normal từ 29 trở đi xác nhận recovery trước khi low evidence đạt 30 giây.

## Giảm false alarm và vòng đời status

Validation, dedup, Late Event filtering, gap continuity, 30 giây low evidence,
recovery 5/10s và một alert/episode là các lớp lọc đã triển khai. Chúng giảm
alert do dữ liệu lỗi, thiếu liên tục hoặc một lần vượt ngưỡng ngắn; không
bảo đảm loại bỏ mọi false alarm ngoài thực tế.

**Prevented warning**: một dip ngắn/chất lượng thấp chưa bao giờ thành alert;
không có alert row để gán status. **`FALSE_ALARM` status**: alert đã tồn tại,
sau đó được phân loại bằng `SEPSIS_ALERT_STATUS_CHANGED`. `ACKNOWLEDGED` là
trạng thái đã ghi nhận, không phải confirmed recovery về sinh lý.

Recovery chỉ reset processing episode, không tự đổi `ACTIVE` sang status
khác, xóa hàng DB hay phát event resolved. Contract hiện có đúng `ACTIVE`,
`ACKNOWLEDGED`, `FALSE_ALARM`. Gateway/Writer nhận status event hợp lệ; chưa
có UI/REST endpoint để người dùng thao tác status. Không tìm thấy alert khi
update thì DLQ `ALERT_NOT_FOUND`, không insert thêm hàng.

`QSOFA_HIGH_RISK` và `SENSOR_DISCONNECTED` là enum được contract/schema hỗ
trợ, **chưa phải rule đang chạy**. Không có GCS để tính qSOFA; không suy
luận sepsis hay lỗi cảm biến từ cross-signal rule chưa triển khai. Lab chưa
được persist/chiếu vào UI hoặc tích hợp BR-6.

Xem [pipeline](../KAFKA_FLINK_FLOW.md), [message contract](KAFKA_MESSAGE_FORMATS.md)
và [bảng alert/Idempotency](DATABASE_DESIGN.md) để đối chiếu đường đi và dữ liệu lưu.
