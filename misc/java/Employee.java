class Person {
    public Person(){
        System.out.println("Person constructor called");
    }
}

public class Employee extends Person{
    public Employee(){
        System.out.println("Employee constructor called");
    }
    public static void main(String[] args){
        Employee e = new Employee();
    }
}
