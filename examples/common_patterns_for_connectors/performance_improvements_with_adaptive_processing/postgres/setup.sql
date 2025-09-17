-- ============================================================================
-- SETUP.SQL - Database Setup for Fivetran Resource Monitoring Connector
-- ============================================================================
-- This file contains all SQL commands necessary to create database objects
-- that simulate the data flow for the resource monitoring connector.
-- 
-- Usage: Execute this file against your PostgreSQL database to set up
-- the required tables, schemas, and sample data.
-- ============================================================================

-- Create schema for the connector
CREATE SCHEMA IF NOT EXISTS fivetran_connector;

-- Set search path to include our schema
SET search_path TO fivetran_connector, public;

-- ============================================================================
-- SAMPLE DATA TABLES (Simulating Source Tables)
-- ============================================================================

-- Sample orders table with region filtering
CREATE TABLE IF NOT EXISTS "order" (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL,
    customer_id INTEGER NOT NULL,
    order_date TIMESTAMP NOT NULL,
    total_amount DECIMAL(10,2) NOT NULL,
    region VARCHAR(50) NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Sample customers table
CREATE TABLE IF NOT EXISTS customer (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL UNIQUE,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE,
    phone VARCHAR(20),
    address TEXT,
    city VARCHAR(100),
    state VARCHAR(50),
    country VARCHAR(50),
    postal_code VARCHAR(20),
    region VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Sample products table
CREATE TABLE IF NOT EXISTS product (
    id SERIAL PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL UNIQUE,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    price DECIMAL(10,2) NOT NULL,
    cost DECIMAL(10,2),
    supplier_id INTEGER,
    region VARCHAR(50),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Sample order_items table
CREATE TABLE IF NOT EXISTS order_item (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    total_price DECIMAL(10,2) NOT NULL,
    region VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Sample suppliers table
CREATE TABLE IF NOT EXISTS supplier (
    id SERIAL PRIMARY KEY,
    supplier_id INTEGER NOT NULL UNIQUE,
    supplier_name VARCHAR(255) NOT NULL,
    contact_name VARCHAR(255),
    email VARCHAR(255),
    phone VARCHAR(20),
    address TEXT,
    city VARCHAR(100),
    state VARCHAR(50),
    country VARCHAR(50),
    region VARCHAR(50),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Sample inventory table
CREATE TABLE IF NOT EXISTS inventory (
    id SERIAL PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL,
    warehouse_id INTEGER NOT NULL,
    quantity_on_hand INTEGER NOT NULL,
    quantity_reserved INTEGER DEFAULT 0,
    reorder_level INTEGER DEFAULT 10,
    reorder_quantity INTEGER DEFAULT 100,
    region VARCHAR(50),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- INDEXES FOR PERFORMANCE
-- ============================================================================

-- Indexes for order table
CREATE INDEX IF NOT EXISTS idx_orders_region ON order(region);
CREATE INDEX IF NOT EXISTS idx_orders_order_date ON order(order_date);
CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON order(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON order(status);

-- Indexes for customer table
CREATE INDEX IF NOT EXISTS idx_customers_region ON customer(region);
CREATE INDEX IF NOT EXISTS idx_customers_email ON customer(email);
CREATE INDEX IF NOT EXISTS idx_customers_city ON customer(city);

-- Indexes for product table
CREATE INDEX IF NOT EXISTS idx_products_region ON product(region);
CREATE INDEX IF NOT EXISTS idx_products_category ON product(category);
CREATE INDEX IF NOT EXISTS idx_products_supplier_id ON product(supplier_id);

-- Indexes for order_item table
CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_item(order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON order_item(product_id);
CREATE INDEX IF NOT EXISTS idx_order_items_region ON order_item(region);

-- Indexes for supplier table
CREATE INDEX IF NOT EXISTS idx_suppliers_region ON supplier(region);
CREATE INDEX IF NOT EXISTS idx_suppliers_country ON supplier(country);

-- Indexes for inventory table
CREATE INDEX IF NOT EXISTS idx_inventory_product_id ON inventory(product_id);
CREATE INDEX IF NOT EXISTS idx_inventory_warehouse_id ON inventory(warehouse_id);
CREATE INDEX IF NOT EXISTS idx_inventory_region ON inventory(region);

-- ============================================================================
-- SAMPLE DATA INSERTION
-- ============================================================================

-- Insert sample customers
INSERT INTO customer (customer_id, first_name, last_name, email, phone, address, city, state, country, postal_code, region) VALUES
(1001, 'John', 'Smith', 'john.smith@email.com', '+1-555-0101', '123 Main St', 'New York', 'NY', 'USA', '10001', 'North America'),
(1002, 'Jane', 'Doe', 'jane.doe@email.com', '+1-555-0102', '456 Oak Ave', 'Los Angeles', 'CA', 'USA', '90210', 'North America'),
(1003, 'Bob', 'Johnson', 'bob.johnson@email.com', '+1-555-0103', '789 Pine Rd', 'Chicago', 'IL', 'USA', '60601', 'North America'),
(1004, 'Alice', 'Brown', 'alice.brown@email.com', '+44-20-1234-5678', '10 Downing St', 'London', 'England', 'UK', 'SW1A 2AA', 'Europe'),
(1005, 'Carlos', 'Garcia', 'carlos.garcia@email.com', '+34-91-123-4567', 'Calle Mayor 123', 'Madrid', 'Madrid', 'Spain', '28013', 'Europe'),
(1006, 'Yuki', 'Tanaka', 'yuki.tanaka@email.com', '+81-3-1234-5678', '1-1-1 Shibuya', 'Tokyo', 'Tokyo', 'Japan', '150-0002', 'Asia Pacific'),
(1007, 'Maria', 'Silva', 'maria.silva@email.com', '+55-11-1234-5678', 'Rua das Flores 456', 'São Paulo', 'SP', 'Brazil', '01234-567', 'Latin America'),
(1008, 'David', 'Wilson', 'david.wilson@email.com', '+61-2-1234-5678', '123 George St', 'Sydney', 'NSW', 'Australia', '2000', 'Asia Pacific');

-- Insert sample supplier
INSERT INTO supplier (supplier_id, supplier_name, contact_name, email, phone, address, city, state, country, region) VALUES
(2001, 'Global Electronics', 'Mike Cn', 'mike.cn@globalelectronics.com', '+1-555-0201', '100 Tech Blvd', 'San Jose', 'CA', 'USA', 'North America'),
(2002, 'European Parts Co', 'Hans Muelr', 'hans.muelr@europeanparts.com', '+49-30-1234-5678', 'Berliner Str 123', 'Berlin', 'Berlin', 'Germany', 'Europe'),
(2003, 'Asia Pacific Supply', 'Kenji Yamoto', 'kenji.yamoto@apsupply.com', '+81-3-9876-5432', 'Ginza 5-1-1', 'Tokyo', 'Tokyo', 'Japan', 'Asia Pacific'),
(2004, 'Latin American Goods', 'Carlos Rodri', 'carlos.rodri@lagoods.com', '+52-55-1234-5678', 'Reforma 123', 'Mexico City', 'DF', 'Mexico', 'Latin America');

-- Insert sample products
INSERT INTO product (product_id, product_name, category, price, cost, supplier_id, region) VALUES
('PROD-001', 'Laptop Computer', 'Electronics', 999.99, 650.00, 2001, 'North America'),
('PROD-002', 'Wireless Mouse', 'Electronics', 29.99, 15.00, 2001, 'North America'),
('PROD-003', 'Office Chair', 'Furniture', 199.99, 120.00, 2002, 'Europe'),
('PROD-004', 'Desk Lamp', 'Furniture', 49.99, 25.00, 2002, 'Europe'),
('PROD-005', 'Smartphone', 'Electronics', 699.99, 450.00, 2003, 'Asia Pacific'),
('PROD-006', 'Tablet', 'Electronics', 399.99, 250.00, 2003, 'Asia Pacific'),
('PROD-007', 'Coffee Maker', 'Appliances', 89.99, 55.00, 2004, 'Latin America'),
('PROD-008', 'Blender', 'Appliances', 59.99, 35.00, 2004, 'Latin America');

-- Insert sample orders
INSERT INTO "order" (order_id, customer_id, order_date, total_amount, region, status) VALUES
('ORD-2024-001', 1001, '2024-01-15 10:30:00', 1029.98, 'North America', 'completed'),
('ORD-2024-002', 1002, '2024-01-16 14:20:00', 699.99, 'North America', 'completed'),
('ORD-2024-003', 1004, '2024-01-17 09:15:00', 249.98, 'Europe', 'completed'),
('ORD-2024-004', 1005, '2024-01-18 16:45:00', 399.99, 'Europe', 'pending'),
('ORD-2024-005', 1006, '2024-01-19 11:30:00', 459.98, 'Asia Pacific', 'completed'),
('ORD-2024-006', 1007, '2024-01-20 13:20:00', 149.98, 'Latin America', 'pending'),
('ORD-2024-007', 1008, '2024-01-21 08:45:00', 999.99, 'Asia Pacific', 'completed'),
('ORD-2024-008', 1003, '2024-01-22 15:10:00', 129.98, 'North America', 'completed');

-- Insert sample order items
INSERT INTO order_item (order_id, product_id, quantity, unit_price, total_price, region) VALUES
('ORD-2024-001', 'PROD-001', 1, 999.99, 999.99, 'North America'),
('ORD-2024-001', 'PROD-002', 1, 29.99, 29.99, 'North America'),
('ORD-2024-002', 'PROD-005', 1, 699.99, 699.99, 'North America'),
('ORD-2024-003', 'PROD-003', 1, 199.99, 199.99, 'Europe'),
('ORD-2024-003', 'PROD-004', 1, 49.99, 49.99, 'Europe'),
('ORD-2024-004', 'PROD-006', 1, 399.99, 399.99, 'Europe'),
('ORD-2024-005', 'PROD-005', 1, 699.99, 699.99, 'Asia Pacific'),
('ORD-2024-005', 'PROD-002', 1, 29.99, 29.99, 'Asia Pacific'),
('ORD-2024-006', 'PROD-007', 1, 89.99, 89.99, 'Latin America'),
('ORD-2024-006', 'PROD-008', 1, 59.99, 59.99, 'Latin America'),
('ORD-2024-007', 'PROD-001', 1, 999.99, 999.99, 'Asia Pacific'),
('ORD-2024-008', 'PROD-002', 2, 29.99, 59.98, 'North America'),
('ORD-2024-008', 'PROD-004', 1, 49.99, 49.99, 'North America'),
('ORD-2024-008', 'PROD-008', 1, 59.99, 59.99, 'North America');

-- Insert sample inventory
INSERT INTO inventory (product_id, warehouse_id, quantity_on_hand, quantity_reserved, reorder_level, reorder_quantity, region) VALUES
('PROD-001', 1, 50, 5, 10, 100, 'North America'),
('PROD-002', 1, 200, 15, 25, 500, 'North America'),
('PROD-003', 2, 30, 3, 5, 50, 'Europe'),
('PROD-004', 2, 150, 10, 20, 300, 'Europe'),
('PROD-005', 3, 75, 8, 15, 200, 'Asia Pacific'),
('PROD-006', 3, 100, 12, 20, 250, 'Asia Pacific'),
('PROD-007', 4, 40, 2, 8, 100, 'Latin America'),
('PROD-008', 4, 80, 6, 15, 150, 'Latin America');

-- ============================================================================
-- VIEWS FOR ANALYSIS
-- ============================================================================

-- View for order summary by region
CREATE OR REPLACE VIEW order_summary_by_region AS
SELECT 
    region,
    COUNT(*) as total_orders,
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_order_value,
    COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_orders,
    COUNT(CASE WHEN status = 'pending' THEN 1 END) as pending_orders
FROM "order"
GROUP BY region
ORDER BY total_revenue DESC;

-- View for customer order history
CREATE OR REPLACE VIEW customer_order_history AS
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    c.region as customer_region,
    COUNT(o.order_id) as total_orders,
    SUM(o.total_amount) as total_spent,
    AVG(o.total_amount) as avg_order_value,
    MAX(o.order_date) as last_order_date
FROM customer c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name, c.region
ORDER BY total_spent DESC;

-- View for product performance
CREATE OR REPLACE VIEW product_performance AS
SELECT 
    p.product_id,
    p.product_name,
    p.category,
    p.region as product_region,
    COUNT(oi.order_id) as times_ordered,
    SUM(oi.quantity) as total_quantity_sold,
    SUM(oi.total_price) as total_revenue,
    AVG(oi.unit_price) as avg_unit_price,
    i.quantity_on_hand as current_stock
FROM product p
LEFT JOIN order_items oi ON p.product_id = oi.product_id
LEFT JOIN inventory i ON p.product_id = i.product_id
GROUP BY p.product_id, p.product_name, p.category, p.region, i.quantity_on_hand
ORDER BY total_revenue DESC;

-- ============================================================================
-- FUNCTIONS FOR DATA GENERATION
-- ============================================================================

-- Function to generate random orders
CREATE OR REPLACE FUNCTION generate_random_orders(num_orders INTEGER DEFAULT 10)
RETURNS VOID AS $$
DECLARE
    i INTEGER;
    random_customer_id INTEGER;
    random_product_id VARCHAR(50);
    random_region VARCHAR(50);
    order_date TIMESTAMP;
    total_amount DECIMAL(10,2);
BEGIN
    FOR i IN 1..num_orders LOOP
        -- Select random customer
        SELECT customer_id INTO random_customer_id 
        FROM customer 
        ORDER BY RANDOM() 
        LIMIT 1;
        
        -- Select random product
        SELECT product_id INTO random_product_id 
        FROM product 
        ORDER BY RANDOM() 
        LIMIT 1;
        
        -- Select random region
        SELECT region INTO random_region 
        FROM (VALUES ('North America'), ('Europe'), ('Asia Pacific'), ('Latin America')) AS regions(region)
        ORDER BY RANDOM() 
        LIMIT 1;
        
        -- Generate random order date in the last 30 days
        order_date := CURRENT_TIMESTAMP - INTERVAL '1 day' * (RANDOM() * 30);
        
        -- Generate random total amount between 50 and 1000
        total_amount := 50 + (RANDOM() * 950);
        
        -- Insert order
        INSERT INTO "order" (order_id, customer_id, order_date, total_amount, region, status)
        VALUES (
            'ORD-' || EXTRACT(YEAR FROM CURRENT_DATE) || '-' || LPAD(i::TEXT, 3, '0'),
            random_customer_id,
            order_date,
            total_amount,
            random_region,
            CASE WHEN RANDOM() > 0.2 THEN 'completed' ELSE 'pending' END
        );
        
        -- Insert order item
        INSERT INTO order_item (order_id, product_id, quantity, unit_price, total_price, region)
        VALUES (
            'ORD-' || EXTRACT(YEAR FROM CURRENT_DATE) || '-' || LPAD(i::TEXT, 3, '0'),
            random_product_id,
            GREATEST(1, FLOOR(RANDOM() * 5)::INTEGER),
            (SELECT price FROM products WHERE product_id = random_product_id),
            total_amount,
            random_region
        );
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Function to update inventory levels
CREATE OR REPLACE FUNCTION update_inventory_levels()
RETURNS VOID AS $$
BEGIN
    UPDATE inventory 
    SET quantity_on_hand = GREATEST(0, quantity_on_hand - FLOOR(RANDOM() * 10)::INTEGER),
        last_updated = CURRENT_TIMESTAMP
    WHERE RANDOM() > 0.7; -- Update 30% of inventory records
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- TRIGGERS FOR DATA INTEGRITY
-- ============================================================================

-- Trigger to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply trigger to tables with updated_at column
CREATE TRIGGER update_customers_updated_at 
    BEFORE UPDATE ON customer 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_products_updated_at 
    BEFORE UPDATE ON product 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_suppliers_updated_at 
    BEFORE UPDATE ON supplier 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- GRANTS AND PERMISSIONS
-- ============================================================================

-- Grant permissions to the fivetran user (adjust username as needed)
-- GRANT USAGE ON SCHEMA fivetran_connector TO fivetran_user;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA fivetran_connector TO fivetran_user;
-- GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA fivetran_connector TO fivetran_user;

-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================

-- Query to verify setup
SELECT 'Setup verification completed successfully' as status;

-- Count records in each table
SELECT 'customers' as table_name, COUNT(*) as record_count FROM customer
UNION ALL
SELECT 'suppliers', COUNT(*) FROM supplier
UNION ALL
SELECT 'products', COUNT(*) FROM product
UNION ALL
SELECT 'orders', COUNT(*) FROM "order"
UNION ALL
SELECT 'order_items', COUNT(*) FROM order_item
UNION ALL
SELECT 'inventory', COUNT(*) FROM inventory
ORDER BY table_name;

-- Show sample data from each table
SELECT 'Sample customers:' as info;
SELECT customer_id, first_name, last_name, region FROM customer LIMIT 3;

SELECT 'Sample orders:' as info;
SELECT order_id, customer_id, total_amount, region, status FROM order LIMIT 3;

SELECT 'Sample products:' as info;
SELECT product_id, product_name, category, price, region FROM product LIMIT 3;
