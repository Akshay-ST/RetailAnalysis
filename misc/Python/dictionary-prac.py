from collections import Counter

l = ['a','a','a','b','b','c','C']

d1 = dict(Counter(l))
d2 = {}
d3 = {}

print(d1)

for item in l:
    if item in d2:
        d2[item] += 1
    else:
        d2[item] = 1

print(d2)

for item in l:
    d3[item] = d3.get(item, 0 ) + 1

print(d3)