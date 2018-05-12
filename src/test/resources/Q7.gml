graph [

  directed 0

  node [
    id 1
    label "{R1}    {X2, X3}"
    vgj [
      labelPosition "in"
      shape "Rectangle"
    ]
  ]

  node [
    id 2
    label "{S1}    {X3, X4}"
    vgj [
      labelPosition "in"
      shape "Rectangle"
    ]
  ]

  node [
    id 3
    label "{S2}    {X4, X5}"
    vgj [
      labelPosition "in"
      shape "Rectangle"
    ]
  ]

  node [
    id 4
    label "{R2}    {X5, X6}"
    vgj [
      labelPosition "in"
      shape "Rectangle"
    ]
  ]

  node [
    id 5
    label "{S3}    {X6, X7}"
    vgj [
      labelPosition "in"
      shape "Rectangle"
    ]
  ]

  node [
    id 6
    label "{R0}    {X1, X2}"
    vgj [
      labelPosition "in"
      shape "Rectangle"
    ]
  ]

  node [
    id 7
    label "{S0}    {X0, X1}"
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

  edge [
    source 1
    target 6
  ]

  edge [
    source 6
    target 7
  ]

]
