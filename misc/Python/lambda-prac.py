# Take input from the user
start = int(input("Enter the start of the range: "))
end = int(input("Enter the end of the range: "))

# Use lambda with filter to get even numbers
even_numbers = list(filter(lambda x: x % 2 == 0, range(start, end + 1)))

# Print the even numbers
print("Even numbers from", start, "to", end, "are:")
print(*even_numbers)


numbers = [1, 2, 3, 4, 5]
print("Original numbers:", numbers)

# Using filter
filtered = list(filter(lambda x: x % 2 == 0, numbers))
# → [2, 4]
print("Filtered even numbers:", filtered)

# Using map
mapped = list(map(lambda x: x * 10, numbers))
# → [10, 20, 30, 40, 50]
print("Mapped numbers multiplied by 10:", mapped)

# Using reduce
from functools import reduce
reduced = reduce(lambda x, y: x + y, numbers)
# → 15
print("Reduced sum of numbers:", reduced)