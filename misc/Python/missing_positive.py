"""
given an array A of N integers, returns the smallest positive integer (greater than 0) that does not occur in A. 
For example, 
given A = [1, 3, 6, 4, 1, 2], the function should return 5. 
Given A = [1, 2, 3], the function should return 4. 
Given A = [−1, −3], the function should return 1.
"""


def solution(A):
    # Convert list to set for O(1) lookups
    seen = set(A)
    
    # Start checking from 1 upwards
    smallest = 1
    
    while True:
        if smallest not in seen:
            return smallest
        smallest += 1

A = [1, 3, 6, 4, 1, 2]
print(solution(A))  # Output: 5 