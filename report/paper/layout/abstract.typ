#import "/layout/fonts.typ": *

#let abstract(body) = {
  set page(
    margin: (left: 30mm, right: 30mm, top: 40mm, bottom: 40mm),
    numbering: none,
    number-align: center,
  )

  set text(
    font: fonts.body, 
    size: 12pt, 
    lang: "vi"
  )

  set par(
    leading: 1em,
    justify: true
  )

  // --- Abstract ---
  v(1fr)
  align(center, text(font: fonts.body, 1em, weight: "semibold", "Tóm tắt"))
  
  body
  
  v(1fr)
}
