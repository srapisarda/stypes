graph [

directed 0

  node [
    id 1
    label "{}    { ?z, ?v }"
    vgj [
      labelPosition "in"
      shape "Rectangle"
    ]
  ]

  node [
    id 2
    label "{}    { ?y, ?z }"
    vgj [
      labelPosition "in"
      shape "Rectangle"
    ]
  ]

  node [
    id 3
    label "{}    { ?x, ?y }"
    vgj [
      labelPosition "in"
      shape "Rectangle"
    ]
  ]

  edge [
    source 2
    target 1
  ]

  edge [
    source 2
    target 3
  ]

]
