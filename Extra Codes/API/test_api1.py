from flask import Flask, jsonify, request

app = Flask(__name__)

# Mock database
employees = {}

@app.route("/", methods=["GET"])
def home():
    return jsonify({"message": "Welcome to the Local API"})

@app.route("/employees", methods=["GET"])
def get_employees():
    return jsonify(employees)

@app.route("/employees/<int:emp_id>", methods=["GET"])
def get_employee(emp_id):
    if emp_id not in employees:
        return jsonify({"error": "Employee not found"}), 404
    return jsonify(employees[emp_id])

@app.route("/employees/<int:emp_id>", methods=["POST"])
def add_employee(emp_id):
    if emp_id in employees:
        return jsonify({"error": "Employee already exists"}), 400
    data = request.get_json()
    employees[emp_id] = data
    return jsonify({"message": "Employee added", "employee": employees[emp_id]})

@app.route("/employees/<int:emp_id>", methods=["PUT"])
def update_employee(emp_id):
    if emp_id not in employees:
        return jsonify({"error": "Employee not found"}), 404
    data = request.get_json()
    employees[emp_id] = data
    return jsonify({"message": "Employee updated", "employee": employees[emp_id]})

@app.route("/employees/<int:emp_id>", methods=["DELETE"])
def delete_employee(emp_id):
    if emp_id not in employees:
        return jsonify({"error": "Employee not found"}), 404
    del employees[emp_id]
    return jsonify({"message": "Employee deleted"})

if __name__ == "__main__":
    app.run(debug=True)
