= TIỀN XỬ LÝ DỮ LIỆU

== Tổng quan về tiền xử lý dữ liệu

Tiền xử lý dữ liệu là bước quan trọng chuyển đổi văn bản thô thành định dạng phù hợp cho mô hình học máy. Đối với tiếng Việt và dữ liệu mạng xã hội, quá trình này cần xử lý các thách thức đặc thù như phân đoạn từ, chuẩn hóa dấu thanh, và xử lý ngôn ngữ không chuẩn.

== Biểu diễn từ bằng vector

=== Nguyên lý cơ bản

Máy tính không thể xử lý trực tiếp văn bản, do đó cần chuyển đổi từ thành vector số. Mỗi từ được biểu diễn như một điểm trong không gian vector đa chiều $bb(R)^d$:

$ w_i arrow.bar bold(v)_i = [v_(i 1), v_(i 2), ..., v_(i d)]^T $

Tính chất quan trọng:

- Từ có nghĩa tương tự có vector gần nhau trong không gian
- Khoảng cách cosine đo độ tương tự: $"similarity"(w_i, w_j) = cos(bold(v)_i, bold(v)_j)$

=== Biểu diễn ngữ cảnh (Contextual Embeddings)

Khác với biểu diễn tĩnh (một từ - một vector), biểu diễn ngữ cảnh tạo vector khác nhau cho cùng một từ tùy theo ngữ cảnh:

$ bold(h)_i = f(bold(x)_1, bold(x)_2, ..., bold(x)_n, i) $

Ví dụ: Từ "đá" có vector khác nhau trong "đá bóng" (động từ) và "viên đá" (danh từ).

== Token hóa (Tokenization) và Byte Pair Encoding (BPE)

=== Khái niệm Token hóa

Token hóa là quá trình chia văn bản thành các đơn vị cơ bản (token) mà mô hình có thể xử lý. Thay vì sử dụng từ hoàn chỉnh, BPE chia từ thành các từ con (subword units).

=== Thuật toán BPE

Nguyên lý: Tìm và gộp các cặp ký tự/từ con xuất hiện nhiều nhất để tạo từ vựng tối ưu.
Các bước thực hiện:

1. Khởi tạo: Từ vựng chứa tất cả ký tự trong kho ngữ liệu
2. Lặp lại:
  - Đếm tần suất các cặp ký tự/từ con liền kề
  - Gộp cặp xuất hiện nhiều nhất thành token mới
  - Cập nhật kho ngữ liệu và từ vựng
3. Dừng: Khi đạt kích thước từ vựng mong muốn

Ví dụ với tiếng Việt:

#align(left)[
*Kho ngữ liệu:* `["học", "học_sinh", "sinh_viên"]`

*Bước 1:* `"h ọ c"`, `"h ọ c _ s i n h"`, `"s i n h _ v i ê n"`

*Bước 2:* Cặp `('h', 'ọ')` xuất hiện 2 lần → gộp thành `'họ'`

*Bước 3:* `"họ c"`, `"họ c _ s i n h"`, `"s i n h _ v i ê n"`

*Bước 4:* Tiếp tục với `('họ', 'c')` → `'học'`
]

#figure(
  table(
    columns: 100%,
    [
      #align(left)[```python
def phobert_bpe_process(segmented_text, vocab_size=64000):
    # Bước 1: Khởi tạo với ký tự
    chars = set()
    for word in segmented_text.split():
        for char in word:
            chars.add(char)
    
    vocab = list(chars)
    
    # Bước 2: Thống kê cặp ký tự trong từ đã phân đoạn
    word_freqs = {}
    for word in segmented_text.split():
        # Thêm </w> để đánh dấu kết thúc từ
        word_with_end = word + '</w>'
        chars = ' '.join(list(word_with_end))
        word_freqs[chars] = word_freqs.get(chars, 0) + 1
    
    # Bước 3: Lặp merge cho đến khi đạt vocab_size
    while len(vocab) < vocab_size:
        pairs = get_pairs(word_freqs)
        if not pairs:
            break
            
        best_pair = max(pairs, key=pairs.get)
        vocab.append(best_pair[0] + best_pair[1])
        
        # Cập nhật word_freqs
        word_freqs = merge_vocab(best_pair, word_freqs)
    
    return vocab, word_freqs
```
      ]
    ]  
  ),
  caption: "Mô phỏng quá trình BPE của PhoBERT"
)

== Xử lý đặc thù tiếng Việt

=== Phân đoạn từ

==== Đặc thù của tiếng Việt

Tiếng Việt là ngôn ngữ đơn lập (isolating language), trong đó ranh giới từ không được đánh dấu rõ ràng bằng khoảng trắng như tiếng Anh. Một từ tiếng Việt có thể bao gồm một hoặc nhiều âm tiết, tạo ra sự mơ hồ trong việc xác định ranh giới từ.

Thống kê về cấu trúc từ tiếng Việt:

#figure(
  table(
    columns: 3,
    table.header(
      [*Loại từ*], [*Tỷ lệ*], [*Ví dụ*]
    ),
    [Từ đơn âm tiết], [15%], ["tôi", "ăn", "đi"],
    [Từ hai âm tiết], [60%], ["học_sinh", "máy_tính", "yêu_thương"],
    [Từ ba âm tiết], [20%], ["đại_học_sinh", "máy_tính_bảng"],
    [Từ bốn âm tiết trở lên], [5%], ["trường_đại_học_bách_khoa"]
  ),
  caption: "Thống kê cấu trúc từ tiếng Việt theo số âm tiết"
)

==== Vấn đề mơ hồ trong phân đoạn từ

Ví dụ về tính mơ hồ:

- *Câu:* Anh ta học ở trường đại học
  - *Cách phân đoạn 1:* Anh ta học ở trường đại học
  - *Cách phân đoạn 2:* Anh ta học ở trường đại_học
  - *Cách phân đoạn 3:* Anh_ta học ở trường đại_học

Tác động đến xử lý NLP:

- Phân đoạn sai → biểu diễn vector sai → hiệu suất mô hình giảm
- Cùng một khái niệm có thể được biểu diễn khác nhau
- Ảnh hưởng đến kích thước vốn từ vựng và độ phức tạp mô hình

=== Phương pháp phân đoạn từ

- Phương pháp dựa trên từ điển
  - Sử dụng từ điển có sẵn để khớp từ dài nhất
  - Thuật toán Maximum Matching (MaxMatch)
- Phương pháp học máy
  - Conditional Random Fields (CRF)
  - Support Vector Machines (SVM)
  - Deep Learning (BiLSTM-CRF)

==== PyVi - Công cụ phân đoạn từ tiếng Việt

*Đặc điểm kỹ thuật:*

- Sử dụng thuật toán Conditional Random Fields (CRF)
- Được huấn luyện trên Vietnamese Treebank
- Đạt F1-score = 0.985 trên tập test chuẩn
- Tốc độ xử lý: ~10,000 từ/giây

#figure(
  table(
    columns: 100%,
    rows: 2,
    [
      #align(left)[
```python
from pyvi import ViTokenizer

def segment_text(text):
    try:
        segmented_text = ViTokenizer.tokenize(text)
        return segmented_text
    except Exception as e:
        print(f"Lỗi phân đoạn từ: {e}")
        return text

# Ví dụ sử dụng
example = "Tôi đang học tại trường đại học bách khoa hà nội"

segmented = segment_text(example)
print(f"Gốc:      {example}")
print(f"Phân đoạn: {segmented}")
print("-" * 50)

```
      ]
    ], [
      #align(left)[
*Gốc:*      Tôi đang học tại trường đại học bách khoa hà nội \
*Phân đoạn:* Tôi đang học tại trường đại_học bách_khoa hà_nội
      ]
    ]
  ),
  caption: "Ví dụ sử dụng PyVi trong thực tế"
)

*So sánh có/không phân đoạn từ:*

- Không phân đoạn: "trường đại học" → 3 token riêng biệt
- Có phân đoạn: "trường đại_học" → "trường" + "đại_học" (2 token có ý nghĩa)

*Lợi ích:*

- Giảm tính mơ hồ trong biểu diễn từ
- Vốn từ vựng có ý nghĩa ngữ nghĩa hơn
- Giảm số lượng token cần thiết cho mô hình huấn luyện


=== Quy trình tiền xử lý tổng quan

==== Kiến trúc quy trình

Quy trình tiền xử lý dữ liệu cho PhoBERT được thiết kế theo kiến trúc đa tầng, mỗi tầng xử lý một khía cạnh cụ thể của văn bản tiếng Việt. Kiến trúc này đảm bảo tính nhất quán và hiệu quả trong việc chuyển đổi từ văn bản thô sang định dạng phù hợp cho mô hình.
Cấu trúc tổng thể:

#import "@preview/fletcher:0.5.8" as fletcher: diagram, node, edge

#figure(
  diagram(
    spacing: (10mm, 8mm),
    edge-stroke: 1pt,
    node-stroke: 1pt,
    node-fill: white,
    node-corner-radius: 3pt,
    node((0, 0), "Văn bản thô"),
    edge("->"),
    node((1, 0), "Chuẩn hóa"),
    edge("->"),
    node((1, 1), "Phân đoạn từ"),
    edge("->"),
    node((0, 2), "Token hóa BPE"),
    edge("->"),
    node((1, 2), "Đầu vào mô hình"),
  ),
  caption: "Kiến trúc quy trình tiền xử lý dữ liệu"
)

==== Phân tích từng giai đoạn

===== Chuẩn hóa văn bản

Giai đoạn chuẩn hóa văn bản xử lý các vấn đề cơ bản của dữ liệu thô:

- Chuẩn hóa Unicode: Chuyển đổi về dạng NFC (Normalization Form Canonical Composition) để đảm bảo tính nhất quán của ký tự tiếng Việt
- Xử lý khoảng trắng: Loại bỏ khoảng trắng thừa và chuẩn hóa xuống dòng
- Chuẩn hóa chữ hoa thường: Chuyển về chữ thường để giảm kích thước từ vựng và tăng tính tổng quát

===== Phân đoạn từ

- Dựa trên đặc tính đơn lập của tiếng Việt, trong đó ranh giới từ không được đánh dấu rõ ràng
- Thuật toán: Sử dụng PyVi với phương pháp dựa trên CRF
- Phân đoạn từ chính xác giúp trình token hóa BPE hoạt động hiệu quả hơn

===== Token hóa BPE

- Thêm các token [CLS], [SEP], [PAD] theo chuẩn BERT

==== Tối ưu hóa quy trình

===== Chiến lược xử lý lô 
Quy trình được thiết kế để xử lý hiệu quả các lô có kích thước khác nhau:

- Tạo lô động: Nhóm các chuỗi có độ dài tương tự để giảm đệm
- Tối ưu hóa bộ nhớ: Xử lý theo khối để tránh tràn bộ nhớ
- Xử lý song song: Các giai đoạn độc lập có thể được song song hóa

== Kết luận chương
Chương này đã trình bày quá trình xây dựng quy trình tiền xử lý dữ liệu cho mô hình phân loại cảm xúc văn bản tiếng Việt sử dụng PhoBERT. Quy trình được thiết kế theo kiến trúc bốn tầng: chuẩn hóa văn bản cơ bản, phân đoạn từ với PyVi, và mã hóa từ BPE. Đối với các đặc thù của tiếng Việt, giải pháp phân đoạn từ được áp dụng để xử lý tính đơn lập của ngôn ngữ, trong khi các kỹ thuật chuẩn hóa được sử dụng để xử lý dấu thanh và ngôn ngữ không chuẩn trong mạng xã hội. Chương cũng trình bày cơ chế mặt nạ chú ý và thẻ đặc biệt trong PhoBERT, bao gồm [CLS], [SEP], [PAD], và [MASK], cùng với vai trò của chúng trong quá trình mã hóa từ. Những phương pháp và giải pháp được trình bày trong chương này tạo cơ sở cho việc triển khai mô hình PhoBERT trong các chương tiếp theo.