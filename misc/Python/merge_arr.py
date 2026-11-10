#arr1 = [1,3,5]
#arr2 = [2,4,6,8]


arr1 = [1, 4, 7] 
arr2 = [2, 3, 8, 10]

def merge_sorted(a, b):
    merged = []
    i = j = 0

    # Compare the current items and take the smaller one
    while i < len(a) and j < len(b):
        if a[i] <= b[j]:
            merged.append(a[i])
            i += 1
        else:
            merged.append(b[j])
            j += 1

    # Add any remaining items
    merged.extend(a[i:])
    merged.extend(b[j:])

    return merged


print(merge_sorted(arr1,arr2))
# [1, 2, 3, 4, 7, 8, 10]
