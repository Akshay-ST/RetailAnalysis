def shift_list(lst, n):
    n = n % len(lst)  # In case n is larger than the list size
    return lst[-n:] + lst[:-n]

# Example usage
lst = [1, 2, 3, 4, 5]
n = 6
shifted_list = shift_list(lst, n)
print(shifted_list)