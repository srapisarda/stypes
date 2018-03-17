import sys

body = False
insideVariableName = False
vars = set()
edges = []
c = sys.stdin.read(1)
while c :
    if c == '-' and d == '<' :
        body = True
    
    if body :
        if c == '(':
            currentEdge = []
    
        if c == '?' :
            insideVariableName = True
            currentVariable = ""
       
        if insideVariableName and (c in ",) "):
            insideVariableName = False
            currentEdge.append(currentVariable)
            vars.add(currentVariable)
            
        if c == ')' :
            edges.append(currentEdge)
        
        if insideVariableName : 
            currentVariable = currentVariable + c
            
    d = c
    c = sys.stdin.read(1)
    
    
############# Printing Answer #######################

for currentVariable in vars:
    print "vertex(", currentVariable, ")."

print

for currentEdge in edges :
    print "edge(", ",".join(currentEdge), ")."
    






