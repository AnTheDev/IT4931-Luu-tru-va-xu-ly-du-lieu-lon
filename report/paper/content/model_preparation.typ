= XÂY DỰNG MÔ HÌNH

== Xây dựng bộ dữ liệu

=== Thu thập dữ liệu

Dữ liệu được thu thập từ UIT-VSMEC, bộ dữ liệu dành riêng cho bài toán nhận diện cảm xúc trong văn bản mạng xã hội Tiếng Việt. 

Bộ dữ liệu này bao gồm *6,927* câu văn bản, mỗi câu được gán nhãn cảm xúc từ các bình luận trên mạng xã hội, bao gồm các nền tảng như Facebook. Các cảm xúc được phân loại theo bảy nhãn: Vui vẻ, Buồn bã, Tức giận, Ngạc nhiên, Ghê tởm, Sợ hãi, và Khác.

Bộ dữ liệu được xây dựng với các bước chuẩn bị dữ liệu nghiêm ngặt, bao gồm việc làm sạch và tiền xử lý văn bản, để đảm bảo độ chính xác cao trong việc gán nhãn cảm xúc. Quá trình thu thập được thực hiện thông qua các công cụ crawling và API để thu thập bình luận từ các bài đăng công khai.

=== Biểu diễn dữ liệu

Cho tập huấn luyện gồm $D={(x_i, y_i) | i=1,2,...,N}$, với:
- $x_i$: câu văn bản tiếng Việt sau xử lý
- $y_i in {0,1,...,C-1}$: nhãn cảm xúc (với $C$ là số nhãn cảm xúc)

== Phương pháp học máy

=== Mô tả bài toán
Bài toán đặt ra là phân loại cảm xúc văn bản tiếng Việt trong ngữ cảnh mạng xã hội. Mỗi đoạn văn bản (câu) là một bình luận hoặc nội dung được đăng tải bởi người dùng, mang sắc thái cảm xúc nhất định. 

Mục tiêu là xây dựng một mô hình học máy có khả năng tự động gán nhãn cảm xúc phù hợp cho từng câu văn bản. Các cảm xúc được phân loại trong bài toán bao gồm: tức giận, buồn bã, ngạc nhiên, vui vẻ, ghê tởm / sợ hãi, khác.

Đây là một bài toán phân loại đa lớp (multi-class classification), thường gặp trong lĩnh vực xử lý ngôn ngữ tự nhiên, đặc biệt là phân tích cảm xúc (sentiment/emotion analysis). 

=== Phát biểu bài toán trên tập dữ liệu
Cho một tập dữ liệu huấn luyện: 

#align(center)[
  $D = {(x_i, y_i) | i =1,...,N}$
]

Trong đó: 

- $x_i$: câu văn bản tiếng Việt sau xử lý 
- $y_i in {0,1,...,6}$: nhãn cảm xúc 
  - $0$: Tức giận 
  - $1$: Buồn bã 
  - $2$: Ngạc nhiên 
  - $3$: Vui vẻ 
  - $4$: Ghê tởm 
  - $5$: Sợ hãi 
  - $6$: Khác 

Yêu cầu:

- Tìm một hàm ánh xạ (hàm phân loại) $f : X arrow Y$, sao cho với mỗi câu văn bản mới $x$, mô hình có thể dự đoán đúng nhãn cảm xúc $y=f(x)$. 

- Hàm $f$ sẽ được học từ tập dữ liệu $D$ bằng cách tối ưu hóa một hàm mất mát, sao cho sai số phân loại trên tập kiểm tra là nhỏ nhất có thể. 

=== Lựa chọn phương pháp học máy
Trong bài toán phân loại cảm xúc văn bản Tiếng Việt, có nhiều phương pháp học máy khác nhau có thể áp dụng. Tuy nhiên, việc lựa chọn phương pháp học máy phù hợp với dữ liệu và bài toán cụ thể này cần phải dựa trên các yếu tố như đặc thù của ngôn ngữ Tiếng Việt, tính chất của các cảm xúc, và yêu cầu về hiệu suất của mô hình. Dưới đây là một số phương pháp học máy thường được sử dụng và lý do lựa chọn mô hình Transformer cho bài toán này.

==== Phương pháp học máy truyền thống
Các phương pháp học máy truyền thống như Naive Bayes, Support Vector Machines (SVM), Logistic Regression, và K-Nearest Neighbors (KNN) thường được sử dụng trong các bài toán phân loại văn bản. 

#figure(
  image(
    "../figures/svm.png",
    width: 50%
  ),
  caption: "Mô hình SVM phân tách siêu phẳng tối ưu trong không gian đặc trưng văn bản"
)

Tuy nhiên, các phương pháp này có một số hạn chế:

- *Thiếu khả năng hiểu ngữ nghĩa sâu sắc*: Các phương pháp này không tận dụng được mối quan hệ ngữ nghĩa giữa các từ trong câu, dẫn đến hiệu suất không cao khi xử lý các ngữ liệu phức tạp, đặc biệt là trong các ngôn ngữ như Tiếng Việt, nơi có các biểu thức cảm xúc phong phú và nhiều lớp nghĩa.

- *Dễ bị ảnh hưởng bởi độ dài câu và cấu trúc văn bản*: Những mô hình này thường không thể tận dụng thông tin ngữ cảnh dài hạn trong câu văn, điều này có thể ảnh hưởng đến độ chính xác của việc phân loại cảm xúc.

==== Phương pháp học sâu (Deep Learning)

Các phương pháp học sâu, đặc biệt là Recurrent Neural Networks (RNN) và Long Short-Term Memory (LSTM), đã được áp dụng thành công trong các bài toán phân loại cảm xúc, nhờ khả năng xử lý các chuỗi dữ liệu và duy trì thông tin ngữ cảnh qua các bước thời gian.


#figure(
  image(
    "../figures/lstm.jpeg",
    width: 90%
  ),
  caption: "Minh họa kiến trúc mạng LSTM để xử lý chuỗi văn bản theo thời gian"
)

Tuy nhiên, mặc dù RNN và LSTM có thể xử lý tốt thông tin tuần tự, chúng có một số nhược điểm:

- *Khó khăn trong việc học các mối quan hệ dài hạn*: Mặc dù LSTM có khả năng giải quyết vấn đề vanishing gradient trong RNN, nhưng việc duy trì thông tin trong các chuỗi dài vẫn là một thử thách.

- *Hiệu suất thấp khi làm việc với các dữ liệu lớn*: Các mô hình này gặp khó khăn khi phải xử lý một lượng dữ liệu lớn và yêu cầu thời gian huấn luyện lâu.

==== Mô hình Transformer và BERT
Mô hình Transformer @vaswani2023 đã cách mạng hóa cách chúng ta xử lý dữ liệu chuỗi và văn bản trong NLP. Mô hình Transformer dựa trên cơ chế self-attention, cho phép mô hình xử lý mọi phần của một câu văn đồng thời mà không cần phải đi qua các bước tuần tự như trong RNN hay LSTM. Các đặc điểm nổi bật của mô hình Transformer là: 

- *Khả năng học các mối quan hệ xa trong văn bản*: Nhờ vào cơ chế attention, Transformer có thể học được các mối quan hệ giữa các từ xa trong câu mà không gặp phải vấn đề vanishing gradient, điều mà các mô hình RNN và LSTM hay gặp phải.

- *Hiệu suất vượt trội*: Các mô hình dựa trên Transformer, như BERT (Bidirectional Encoder Representations from Transformers), đã đạt được thành tựu vượt trội trong nhiều bài toán NLP, bao gồm phân loại văn bản, nhận diện thực thể, và phân tích cảm xúc. BERT học được ngữ cảnh từ cả hai hướng (trái sang phải và phải sang trái), giúp mô hình hiểu rõ hơn về nghĩa của từ trong câu.

Với các lý do trên, mô hình BERT được chọn để xử lý bài toán phân loại cảm xúc văn bản Tiếng Việt. 

== Kết luận chương
Chương này đã trình bày quá trình xây dựng mô hình phân loại cảm xúc văn bản Tiếng Việt, bao gồm việc thu thập và chuẩn bị bộ dữ liệu, cũng như lựa chọn phương pháp học máy phù hợp. Bộ dữ liệu UIT-VSMEC được thu thập từ các bình luận trên mạng xã hội và được gán nhãn cảm xúc theo 7 lớp: Vui vẻ, Buồn bã, Tức giận, Ngạc nhiên, Ghê tởm, Sợ hãi và Khác. Quá trình tiền xử lý dữ liệu bao gồm việc làm sạch và chuẩn hóa văn bản, đảm bảo độ chính xác trong việc gán nhãn.

Việc lựa chọn phương pháp học máy được thực hiện dựa trên các đặc thù của ngôn ngữ Tiếng Việt và yêu cầu về hiệu suất của mô hình. Các phương pháp học máy truyền thống như Naive Bayes và SVM có thể được áp dụng, nhưng không khai thác đầy đủ các mối quan hệ ngữ nghĩa trong văn bản, điều này đặc biệt quan trọng đối với Tiếng Việt với tính phức tạp trong biểu đạt cảm xúc. Các phương pháp học sâu như RNN và LSTM, mặc dù hiệu quả trong việc xử lý dữ liệu tuần tự, vẫn có hạn chế về khả năng duy trì thông tin ngữ cảnh dài hạn và hiệu suất với tập dữ liệu lớn.

Mô hình Transformer, đặc biệt là BERT (Bidirectional Encoder Representations from Transformers), được chọn để giải quyết bài toán phân loại cảm xúc. BERT có khả năng học các mối quan hệ xa trong văn bản và đạt hiệu suất vượt trội trong các bài toán NLP. Với cơ chế học ngữ cảnh từ cả hai hướng (trái sang phải và phải sang trái), BERT giúp mô hình hiểu sâu hơn về ngữ nghĩa của từ trong câu, từ đó nâng cao độ chính xác trong việc phân loại cảm xúc văn bản Tiếng Việt.

Kết quả của chương này cung cấp nền tảng cho việc triển khai và huấn luyện mô hình trong các chương tiếp theo.
