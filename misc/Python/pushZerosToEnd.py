def pushZerosToEnd(self,arr):
    last=-1

    for i in range(len(arr)):
        if arr[i] == 0 and i != (len(arr)-1):

            arr[i],arr[last]=arr[last],arr[i]
            last-=1
    return arr

inp = [3,4,0,0,5]

out = pushZerosToEnd(inp)
print(out)  # Output: [3, 4, 5, 0, 0]


inp2 = [0, 1, 0, 3, 12]
out2 = pushZerosToEnd(inp2)
print(out2)  # Output: [1, 3, 12, 0, 0]
inp3 = [0, 0, 0, 0]
out3 = pushZerosToEnd(inp3)
print(out3)  # Output: [0, 0, 0, 0