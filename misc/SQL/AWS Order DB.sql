CREATE DATABASE IF NOT EXISTS amazon_orders_db;
USE amazon_orders_db;

--Dim Date Table
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,
    date DATE,
    day INT,
    month INT,
    year INT,
    weekday VARCHAR(10),
    is_weekend VARCHAR(3)
);

INSERT INTO dim_date VALUES
(20230101, '2023-01-01', 1, 1, 2023, 'Sunday', 'Yes'),
(20230102, '2023-01-02', 2, 1, 2023, 'Monday', 'No'),
(20230103, '2023-01-03', 3, 1, 2023, 'Tuesday', 'No'),
(20230104, '2023-01-04', 4, 1, 2023, 'Wednesday', 'No'),
(20230105, '2023-01-05', 5, 1, 2023, 'Thursday', 'No'),
(20230106, '2023-01-06', 6, 1, 2023, 'Friday', 'No'),
(20230107, '2023-01-07', 7, 1, 2023, 'Saturday', 'Yes');

select * from dim_date

--Dim Customer Table 
CREATE TABLE dim_customer (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(20),
    country VARCHAR(50),
    state VARCHAR(50),
    city VARCHAR(50),
    zip_code VARCHAR(20),
    signup_date DATE
);

INSERT INTO dim_customer VALUES
(1, 'John Doe', 'john@example.com', '1234567890', 'USA', 'California', 'Los Angeles', '90001', '2021-01-15'),
(2, 'Alice Smith', 'alice@example.com', '1234509876', 'USA', 'Texas', 'Dallas', '75001', '2021-03-10'),
(3, 'Bob Lee', 'bob@example.com', '1231231234', 'Canada', 'Ontario', 'Toronto', 'M5H', '2022-02-22');

select * from dim_customer

--Dim Product Table
CREATE TABLE dim_product (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(50),
    subcategory VARCHAR(50),
    brand VARCHAR(50),
    model_number VARCHAR(50),
    launch_date DATE
);

INSERT INTO dim_product VALUES
(1, 'iPhone', 'Electronics', 'Smartphones', 'Apple', 'MOD-1234', '2022-01-01'),
(2, 'Galaxy Watch', 'Electronics', 'Wearables', 'Samsung', 'MOD-5678', '2022-03-15'),
(3, 'Nike Shoes', 'Fashion', 'Footwear', 'Nike', 'MOD-4321', '2021-11-10'),
(4, 'Dell Laptop', 'Electronics', 'Computers', 'Dell', 'MOD-8765', '2020-05-20'),
(5, 'Sony Headphones', 'Electronics', 'Audio', 'Sony', 'MOD-1357', '2021-08-30'),
(6, 'Adidas Jacket', 'Fashion', 'Apparel', 'Adidas', 'MOD-2468', '2022-02-01'),
(7, 'Canon Camera', 'Electronics', 'Cameras', 'Canon', 'MOD-3690', '2021-12-05'),
(8, 'HP Printer', 'Electronics', 'Printers', 'HP', 'MOD-2580', '2020-10-10'),
(9, 'Samsung TV', 'Electronics', 'Televisions', 'Samsung', 'MOD-1470', '2021-09-15'),
(10, 'Bose Speaker', 'Electronics', 'Audio', 'Bose', 'MOD-3691', '2022-04-20');

select * from dim_product

--Dim Seller Table
CREATE TABLE dim_seller (
    seller_id INT PRIMARY KEY,
    seller_name VARCHAR(100),
    rating DECIMAL(3,1),
    city VARCHAR(50),
    state VARCHAR(50),
    join_date DATE
);

INSERT INTO dim_seller VALUES
(1, 'BestBuy', 4.5, 'New York', 'NY', '2020-01-01'),
(2, 'ElectroWorld', 4.2, 'San Jose', 'CA', '2021-06-15'),
(3, 'ShoeMart', 3.8, 'Chicago', 'IL', '2019-11-05'),
(4, 'Techie', 4.7, 'Los Angeles', 'CA', '2020-03-20'),
(5, 'FashionHub', 4.0, 'Houston', 'TX', '2021-02-10'),
(6, 'GadgetGalaxy', 4.3, 'Seattle', 'WA', '2018-08-25'),
(7, 'CameraWorld', 4.6, 'Miami', 'FL', '2022-01-30'),
(8, 'PrintMasters', 3.9, 'Boston', 'MA', '2019-05-15'),
(9, 'TVLand', 4.1, 'Atlanta', 'GA', '2020-07-22'),
(10, 'AudioExperts', 4.8, 'Phoenix', 'AZ', '2021-04-05');

select * from dim_seller

--Dim Shipping Table
CREATE TABLE dim_shipping (
    shipping_id INT PRIMARY KEY,
    shipping_method VARCHAR(50),
    carrier VARCHAR(50),
    shipping_cost DECIMAL(10,2),
    shipping_time_days INT
);

INSERT INTO dim_shipping VALUES
(1, 'Standard', 'FedEx', 30.00, 5),
(2, 'Prime', 'Amazon Logistics', 0.00, 2),
(3, 'Express', 'DHL', 50.00, 3),
(4, 'Overnight', 'UPS', 70.00, 1),
(5, 'International', 'USPS', 100.00, 7),
(6, 'Economy', 'FedEx', 20.00, 10),
(7, 'Two-Day', 'Amazon Logistics', 15.00, 2),
(8, 'Same-Day', 'DHL', 80.00, 0),
(9, 'Scheduled Delivery', 'UPS', 40.00, 4),
(10, 'Local Pickup', 'N/A', 0.00, 0);

select * from dim_shipping

--Dim Payment Table
CREATE TABLE dim_payment (
    payment_id INT PRIMARY KEY,
    payment_method VARCHAR(50),
    bank_name VARCHAR(100),
    transaction_status VARCHAR(50)
);

INSERT INTO dim_payment VALUES
(1, 'Credit Card', 'HDFC Bank', 'Success'),
(2, 'UPI', 'PhonePe', 'Success'),
(3, 'COD', NULL, 'Success'),
(4, 'Debit Card', 'ICICI Bank', 'Failed'),
(5, 'Net Banking', 'SBI', 'Success'),
(6, 'Wallet', 'Paytm', 'Success'),
(7, 'EMI', 'Axis Bank', 'Pending'),
(8, 'Gift Card', NULL, 'Success'),
(9, 'PayPal', NULL, 'Success'),
(10, 'Crypto', NULL, 'Failed');

select * from dim_payment

--FACT Orders Table
CREATE TABLE fact_orders (
    order_id INT PRIMARY KEY,
    order_date_key INT,
    customer_id INT,
    product_id INT,
    seller_id INT,
    shipping_id INT,
    payment_id INT,
    quantity INT,
    unit_price DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    tax_amount DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    order_status VARCHAR(50),
    
    FOREIGN KEY (order_date_key)    REFERENCES dim_date(date_key),
    FOREIGN KEY (customer_id)       REFERENCES dim_customer(customer_id),
    FOREIGN KEY (product_id)        REFERENCES dim_product(product_id),
    FOREIGN KEY (seller_id)         REFERENCES dim_seller(seller_id),
    FOREIGN KEY (shipping_id)       REFERENCES dim_shipping(shipping_id),
    FOREIGN KEY (payment_id)        REFERENCES dim_payment(payment_id)
);


INSERT INTO fact_orders VALUES
(1, 20230101, 1, 1, 1, 1, 1, 1, 999.99, 100.00, 162.00, 1061.99, 'Delivered');
INSERT INTO fact_orders VALUES
(2, 20230102, 2, 2, 2, 2, 2, 2, 299.99, 30.00, 97.20, 667.18, 'Shipped'),
(3, 20230103, 3, 3, 3, 3, 3, 1, 499.99, 50.00, 80.98, 530.97, 'Cancelled');

insert into fact_orders values
(4, 20230104, 1, 4, 4, 4, 4, 1, 799.99, 80.00, 128.00, 847.99, 'Delivered'),
(5, 20230105, 2, 5, 5, 5, 5, 3, 199.99, 20.00, 32.40, 212.59, 'Returned');
INSERT INTO fact_orders VALUES
(6, 20230106, 3, 6, 6, 6, 6, 2, 129.99, 10.00, 20.80, 140.79, 'Delivered'),
(7, 20230107, 1, 7, 7, 7, 7, 1, 599.99, 60.00, 97.20, 637.19, 'Shipped');

select * from fact_orders


SELECT
    f.*,
    s.*
from fact_orders f
join dim_shipping s on f.shipping_id = s.shipping_id
where s.carrier = 'DHL'