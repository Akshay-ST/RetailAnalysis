------
solution
def is_square(matrix):
    # Check if the matrix is square
    num_rows = len(matrix)
    for row in matrix:
        if len(row) != num_rows:
            return False, 0
    return True, num_rows

def diagonal_sums(matrix):
    is_square_matrix, size = is_square(matrix)
    if not is_square_matrix:
        return "The matrix is not square."
    
    primary_diagonal_sum = 0
    secondary_diagonal_sum = 0
    
    for i in range(size):
        primary_diagonal_sum += matrix[i][i]
        secondary_diagonal_sum += matrix[i][size - i - 1]
    
    return primary_diagonal_sum, secondary_diagonal_sum

# Example usage:
matrix = [
    [1, 2, 3],
    [4, 5, 6]
]

result = diagonal_sums(matrix)
if isinstance(result, tuple):
    print(f"Primary Diagonal Sum: {result[0]}, Secondary Diagonal Sum: {result[1]}")
else:
    print(result)
					
