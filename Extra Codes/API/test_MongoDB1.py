from flask import Flask, jsonify, request
from pymongo import MongoClient

app = Flask(__name__)

# Connect to MongoDB
client = MongoClient("mongodb://127.0.0.1:27017/")
db = client["Practice"]
collection = db["employees"]

@app.route("/", methods=["GET"])
def home():
    return jsonify({"message": "Welcome to the Local API"})

@app.route("/employees", methods=["GET"])
def get_employees():
    employees = list(collection.find({}, {"_id": 0}))
    return jsonify(employees)

@app.route("/employees/<int:emp_id>", methods=["GET"])
def get_employee(emp_id):
    employee = collection.find_one({"emp_id": emp_id}, {"_id": 0})
    if not employee:
        return jsonify({"error": "Employee not found"}), 404
    return jsonify(employee)

@app.route("/employees/<int:emp_id>", methods=["POST"])
def add_employee(emp_id):
    if collection.find_one({"emp_id": emp_id}):
        return jsonify({"error": "Employee already exists"}), 400
    data = request.get_json()
    data["emp_id"] = emp_id
    collection.insert_one(data)
    return jsonify({"message": "Employee added", "employee": data})

@app.route("/employees/<int:emp_id>", methods=["PUT"])
def update_employee(emp_id):
    if not collection.find_one({"emp_id": emp_id}):
        return jsonify({"error": "Employee not found"}), 404
    data = request.get_json()
    collection.update_one({"emp_id": emp_id}, {"$set": data})
    return jsonify({"message": "Employee updated", "employee": data})

@app.route("/employees/<int:emp_id>", methods=["DELETE"])
def delete_employee(emp_id):
    result = collection.delete_one({"emp_id": emp_id})
    if result.deleted_count == 0:
        return jsonify({"error": "Employee not found"}), 404
    return jsonify({"message": "Employee deleted"})

if __name__ == "__main__":
    app.run(debug=True)
