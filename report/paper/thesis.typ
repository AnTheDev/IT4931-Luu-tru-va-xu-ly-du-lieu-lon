#import "/layout/thesis_template.typ": *
#import "/metadata.typ": *

#set document(title: title, author: author)

#show: thesis.with(
  title: title,
  subject: subject,
  subject_description: subject_description,
  supervisor: supervisor,
  advisors: advisors,
  author: author,
  submissionDate: submissionDate,
  abstract_content: include "/content/abstract.typ",
)

#set par(justify: true)
#include "/content/introduction.typ"
#pagebreak()
#include "/content/model_architecture.typ"
#pagebreak()
#include "/content/model_implementation.typ"
#pagebreak()
#include "/content/model_evaluation.typ"
#pagebreak()
#include "/content/model_application.typ"
#pagebreak()
#include "/content/conclusion.typ"
// #include "/content/background.typ"
// #include "/content/related_work.typ"
// #include "/content/requirements.typ"
// #include "/content/system_design.typ"
// #include "/content/evaluation.typ"
// #include "/content/summary.typ"
