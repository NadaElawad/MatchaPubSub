-- Schema for Matcha Café Database

-- 1. Products Table (What we offer & prices)
CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE NOT NULL,
    category VARCHAR(50) NOT NULL DEFAULT 'Drink',
    price DECIMAL(6, 2) NOT NULL,
    description TEXT,
    in_stock BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 2. Orders Table (Records of every order)
CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(50) UNIQUE NOT NULL,
    customer_name VARCHAR(100) NOT NULL,
    drink_name TEXT NOT NULL,
    milk VARCHAR(50),
    sweetness VARCHAR(50),
    price DECIMAL(10, 2) NOT NULL,
    cup_code TEXT,
    cup_codes TEXT[],
    items JSONB,
    status VARCHAR(50) DEFAULT 'READY',
    prepared_by VARCHAR(100),
    ordered_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    ready_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    dining_option VARCHAR(50) DEFAULT 'dine_in'
);

-- 3. Customers Table (Preferences, spending & loyalty aggregated)
CREATE TABLE IF NOT EXISTS customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE NOT NULL,
    favorite_drink TEXT,
    preferred_milk VARCHAR(50),
    preferred_sweetness VARCHAR(50),
    total_spent DECIMAL(10, 2) DEFAULT 0.00,
    total_orders INT DEFAULT 0,
    last_visit TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 4. Cup Inventory & Tracking Table
CREATE TABLE IF NOT EXISTS cups (
    id SERIAL PRIMARY KEY,
    cup_code VARCHAR(20) UNIQUE NOT NULL,
    material VARCHAR(50) NOT NULL DEFAULT 'Ceramic',
    status VARCHAR(50) NOT NULL DEFAULT 'CLEAN_ON_SHELF',
    current_order_id VARCHAR(50),
    current_customer VARCHAR(100),
    total_uses INT DEFAULT 0,
    last_washed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 5. Cup Audit Log (Traceability & History)
CREATE TABLE IF NOT EXISTS cup_audit_log (
    id SERIAL PRIMARY KEY,
    cup_code VARCHAR(20) NOT NULL,
    from_status VARCHAR(50) NOT NULL,
    to_status VARCHAR(50) NOT NULL,
    order_id VARCHAR(50),
    actor VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Seed initial products menu
INSERT INTO products (name, category, price, description) VALUES
    ('Iced Ceremonial Matcha Latte', 'Drink', 6.50, 'Stone-ground Uji matcha whisked over cold milk and artisan ice'),
    ('Hot Uji Matcha Latte', 'Drink', 6.00, 'Steamed milk with vibrant green ceremonial matcha foam'),
    ('Strawberry Matcha Float', 'Drink', 7.50, 'House-made strawberry purée layered with oat milk and cold-whisked matcha'),
    ('Matcha Espresso Fusion', 'Drink', 6.75, 'A double shot of espresso floating over iced layered matcha latte'),
    ('Matcha Soft Serve', 'Dessert', 4.50, 'Creamy Hokkaido-style matcha soft serve ice cream'),
    ('Matcha Basque Cheesecake', 'Pastry', 8.00, 'Rich, caramelized crust with an oozing matcha center')
ON CONFLICT (name) DO UPDATE SET price = EXCLUDED.price;

-- Seed initial pool of 12 handcrafted ceramic cups
INSERT INTO cups (cup_code) VALUES
    ('CUP-01'), ('CUP-02'), ('CUP-03'), ('CUP-04'), ('CUP-05'), ('CUP-06'),
    ('CUP-07'), ('CUP-08'), ('CUP-09'), ('CUP-10'), ('CUP-11'), ('CUP-12')
ON CONFLICT (cup_code) DO NOTHING;
