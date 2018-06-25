graph [

directed 0

  node [
    id 1
    label "{}    { ?x0, ?x1 }"
    vgj [
      labelPosition "in"
      shape "Rectangle"
    ]
  ]

  node [
    id 2
    label "{}    { ?x2, ?x1 }"
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

  node [
    id 4
    label "{}    { ?x3, ?x4 }"
    vgj [
      labelPosition "in"
      shape "Rectangle"
    ]
  ]

  node [
    id 5
    label "{}    { ?x4, ?x5 }"
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

  edge [
    source 3
    target 4
  ]

  edge [
    source 4
    target 5
  ]

]
