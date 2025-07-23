/*
Write a python code to split an array of N integers into N/2 so that sum of integers in each pair is odd. N is even and every element of the array must be present in exactly one pair

expected output is to return true or false if it is possible to split such a array
*/

def split_array(arr):
    # Separate odd and even numbers
    odd_nums = [num for num in arr if num % 2 != 0]
    even_nums = [num for num in arr if num % 2 == 0]

    # Pair odd numbers with even numbers
    pairs = []
    for i in range(len(odd_nums)):
        pairs.append((odd_nums[i], even_nums[i]))

    return pairs

# Test the function
arr = [1, 2, 3, 4, 5, 6]
print(split_array(arr))

--------------------------------

def can_split_array(arr):
    # Count the number of odd and even numbers
    odd_count = sum(1 for num in arr if num % 2 != 0)
    even_count = len(arr) - odd_count

    # It is possible to split the array if and only if the counts are equal
    return odd_count == even_count

# Test the function
arr = [1, 2, 3, 4, 5, 6]
print(can_split_array(arr))  # Output: True

arr = [1, 2, 3, 4, 5]
print(can_split_array(arr))  # Output: False


