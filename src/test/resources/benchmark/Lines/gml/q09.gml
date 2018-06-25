graph [

directed 0

  node [
    id 1
    label "{}    { ?x6, ?x5 }"
    vgj [
      labelPosition "in"
      shape "Rectangle"
    ]
  ]

  node [
    id 2
    label "{}    { ?x4, ?x5 }"
    vgj [
      labelPosition "in"
      shape "Rectangle"
    ]
  ]

  node [
    id 4
    label "{}    { ?x4, ?x3 }"
    vgj [
      labelPosition "in"
      shape "Rectangle"
    ]
  ]

  node [
    id 6
    label "{}    { ?x2, ?x3 }"
    vgj [
      labelPosition "in"
      shape "Rectangle"
    ]
  ]

  node [
    id 8
    label "{}    { ?x2, ?x1 }"
    vgj [
      labelPosition "in"
      shape "Rectangle"
    ]
  ]

  node [
    id 9
    label "{}    { ?x0, ?x1 }"
    vgj [
      labelPosition "in"
      shape "Rectangle"
    ]
  ]

  node [
    id 3
    label "{}    { ?x6, ?x7 }"
    vgj [
      labelPosition "in"
      shape "Rectangle"
    ]
  ]

  node [
    id 5
    label "{}    { ?x8, ?x7 }"
    vgj [
      labelPosition "in"
      shape "Rectangle"
    ]
  ]

  node [
    id 7
    label "{}    { ?x8, ?x9 }"
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
    target 4
  ]

  edge [
    source 4
    target 6
  ]

  edge [
    source 6
    target 8
  ]

  edge [
    source 8
    target 9
  ]

  edge [
    source 1
    target 3
  ]

  edge [
    source 3
    target 5
  ]

  edge [
    source 5
    target 7
  ]

]
