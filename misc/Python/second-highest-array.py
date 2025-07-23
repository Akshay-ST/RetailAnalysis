def second_highest(arr):
    """
    Returns the third highest element from a numeric array.
    - If the array has less than 3 elements, returns -1.
    - If all elements are the same, returns that element.
    - Otherwise, returns the third highest unique element.
    """
    if len(arr) < 3:
        return -1
    unique_arr = list(set(arr)) # Convert to set to remove duplicates
    if len(unique_arr) == 1:    # All elements are the same
        return unique_arr[0]
    if len(unique_arr) < 3:     # Less than 3 unique elements   
        return -1
    unique_arr.sort(reverse=True) # Sort in descending order
    return unique_arr[2]        # Return the third highest element

print(second_highest([1, 2, 3, 4, 5]))  # Output: 3
print(second_highest([5, 5, 5, 5]))     # Output: 5
print(second_highest([1, 2]))           # Output: -1
print(second_highest([2, 2, 3, 3, 4]))  # Output: 2