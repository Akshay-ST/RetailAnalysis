class ListNode:
    def __init__(self, val=0, next=None):
        self.val = val
        self.next = next

def create_linked_list_from_input():
    # Prompt user for input
    values = input("Enter values separated by spaces: ").split()
    # Convert input strings to integers
    values = list(map(int, values))
    
    # Create the head of the linked list
    head = None
    current = None
    
    for val in values:
        new_node = ListNode(val)
        if head is None:
            head = new_node
            current = new_node
        else:
            current.next = new_node
            current = new_node
    
    return head

# Example usage
head = create_linked_list_from_input()

while head is not None:
    print(head.val)
    head = head.next