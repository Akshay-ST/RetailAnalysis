/*
Given an array of words like "Hello", "World" Etc and another array of characters like "k", "W"
Return those word from array of words those contain characters in 2nd array of characters
*/

def find_words_containing_chars(words, chars):
    # Convert the list of characters to a set for efficient lookup
    char_set = set(chars)
    
    # Filter and return words that contain any of the characters in char_set
    result = [word for word in words if any(char in char_set for char in word)]
    
    return result

# Example usage:
words = ["Hello", "World", "Python", "Programming"]
chars = ["k", "W", "o"]
result = find_words_containing_chars(words, chars)
print(result)

