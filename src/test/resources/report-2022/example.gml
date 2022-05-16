graph [

directed 0

  node [
    id 1
    label "{}    { ?x1, ?x0 }"
    vgj [
      labelPosition "in"
      shape "Rectangle"
    ]
  ]

  node [
    id 2
    label "{}    { ?x1, ?x2 }"
    vgj [
      labelPosition "in"
      shape "Rectangle"
    ]
  ]

  node [
    id 3
    label "{}    { ?x3, ?x2 }"
    vgj [
      labelPosition "in"
      shape "Rectangle"
    ]
  ]

  edge [
    source 1
    target 2
  ]

  edge [
    source 2
    target 3
  ]

]
