// Step 1: Define the custom exception
class InvalidAgeException extends Exception {
    public InvalidAgeException(String message) {
        super(message);
    }
}

// Step 2: Use the exception
public class VoterCheck {
    public static void validate(int age) throws InvalidAgeException {
        if (age < 18) {
            // Using 'throw' to trigger the exception
            throw new InvalidAgeException("Not eligible to vote.");
        }
    }

    public static void main(String[] args) {
        try {
            validate(16);
        } catch (InvalidAgeException e) {
            System.out.println("Caught: " + e.getMessage());
        }
    }
}