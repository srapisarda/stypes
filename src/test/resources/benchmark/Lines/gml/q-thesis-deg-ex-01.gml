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
    label "{}    { ?x1, ?x2 }"
    vgj [
      labelPosition "in"
      shape "Rectangle"
    ]
  ]

  node [
    id 3
    label "{}    { ?x2, ?x3}"
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
    label "{}    { ?x0, ?x5 }"
    vgj [
      labelPosition "in"
      shape "Rectangle"
    ]
  ]

  node [
    id 6
    label "{}    { ?x5, ?x6 }"
    vgj [
      labelPosition "in"
      shape "Rectangle"
    ]
  ]

  node [
    id 7
    label "{}    { ?x6, ?x7 }"
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
    source 1
    target 5
  ]

  edge [
    source 5
    target 6
  ]

  edge [
    source 6
    target 7
  ]

]
