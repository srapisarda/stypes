import sys

# Functions for "Human" format processing 

def getNumber(line):
    i = line.find("E ")
    j = line.find(":")
    n = int(line[i+2 : j])
    return n

class Bag:
    _variables = []
    def __init__(self, line):
        i = line.find('[')
        j = line.find(']')
        variables = line[ i + 1 : j ].split(",")
        stripped_variables = [var.strip() for var in variables]
        self._variables = stripped_variables
    def getVariables(self):
        return self._variables

def getLevel(line):
    c = 0 
    while line[c] == ' ' :
        c += 1
    level = c / 3
    return int(level)

# Getting the file name from command line arguments

if (len(sys.argv) < 2):
    print("Usage: python transform-td.py filename.htd")
    exit()
else:
    filename = sys.argv[1]
    
    
# Computing the graph

bags = dict()
edges = []
stack = []
f = open(filename, "r")
for line in f :
    n = getNumber(line)
    bags[n] = Bag(line)
    level = getLevel(line)
    if len(stack) > level :
        stack = stack[:level]
    if len(stack) > 0:
        edges.append((stack[-1], n))
    stack.append(n)

# Defining printing functions
def printNode(n):
    print('  node [')
    print('    id', n)
    print('    label "{}    {', ', '.join(bags[n].getVariables() ), '}"')
    print('    vgj [')
    print('      labelPosition "in"')
    print('      shape "Rectangle"');
    print('    ]')
    print('  ]')
    print()
    
def printEdge(i,j):
    print('  edge [')
    print('    source',i) 
    print('    target',j)
    print('  ]')
    print()


    
# Printing the graph
    
print('graph [')
print()
print('directed 0')
print()

for n in bags :
    printNode(n)
    
for (i,j) in edges:
    printEdge(i,j)

print(']')
    


    
    
#    stack.append(Bag(line))
  
    
