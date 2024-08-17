from collections import Counter

def find_numbers_repeated_three_times(arr):
    # Count the frequency of each number in the array
    frequency = Counter(arr)
    
    # Filter and return the numbers that appear exactly three times
    result = [num for num, count in frequency.items() if count == 3]
    
    return result

# Example usage:
arr = [1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5]
result = find_numbers_repeated_three_times(arr)
print(result)



------------------------------

def find_numbers_repeated_three_times(arr):
    # Create a dictionary to store the frequency of each number
    frequency = {}
    
    # Count the frequency of each number in the array
    for num in arr:
        if num in frequency:
            frequency[num] += 1
        else:
            frequency[num] = 1
    
    # Filter and return the numbers that appear exactly three times
    result = [num for num, count in frequency.items() if count == 3]
    
    return result

# Example usage:
arr = [1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5]
result = find_numbers_repeated_three_times(arr)
print(result)

