#import "/layout/fonts.typ": *

#let cover(
  title: "",
  subject: "",
  subject_description: "",
  author: "",
) = {
  set page(
    margin: (left: 30mm, right: 30mm, top: 40mm, bottom: 40mm),
    numbering: none,
    number-align: center,
  )

  set text(
    font: fonts.body, 
    size: 12pt, 
  )
  
  set par(leading: 1em)

  
  // --- Cover ---
  align(center, text(font: fonts.sans, 2em, weight: 700,   " ĐẠI HỌC BÁCH KHOA HÀ NỘI"))

  align(center, text(font: fonts.sans, 1.5em, weight: 100,   " Trường Công nghệ Thông tin và Truyền thông \n ------------ 🏵 ------------"))

  v(15mm)
  align(center, image("../figures/hust_logo.svg", width: 26%))

  v(10mm)
  align(center, text(font: fonts.sans, 2em, weight: 700, title))
  
  v(10mm)
  align(center, text(font: fonts.sans, 1.8em, weight: 100, subject))

  align(center, text(font: fonts.sans, 1.5em, weight: 100, subject_description))
  
  v(15mm)
  align(center, text(font: fonts.sans, 2em, weight: 500, author))
}