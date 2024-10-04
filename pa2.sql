USE pa2;

CREATE TABLE IF NOT EXISTS opt_clients (
    id CHAR(36) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    surname VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    phone VARCHAR(50) NOT NULL,
    address TEXT NOT NULL,
    status ENUM('active', 'inactive') NOT NULL
);

CREATE TABLE IF NOT EXISTS opt_products (
    product_id INT AUTO_INCREMENT PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    product_category ENUM('Category1', 'Category2', 'Category3', 'Category4', 'Category5') NOT NULL,
    description TEXT
);

CREATE TABLE IF NOT EXISTS opt_orders (
    order_id INT AUTO_INCREMENT PRIMARY KEY,
    order_date DATE NOT NULL,
    client_id CHAR(36),
    product_id INT,
    FOREIGN KEY (client_id) REFERENCES opt_clients(id),
    FOREIGN KEY (product_id) REFERENCES opt_products(product_id)
);

-- EXPLAIN ANALYZE --
SELECT CONCAT(pcc.product_category, ': ', pcc.order_count) AS category_order_count
FROM (
    SELECT 
        client_orders.product_category, 
        COUNT(client_orders.client_name) AS order_count
    FROM (
        SELECT 
            CONCAT(c.name, ' ', c.surname) AS client_name, 
            c.status, 
            p.product_name, 
            p.product_category, 
            o.order_date
        FROM opt_orders o
        JOIN opt_clients c ON o.client_id = c.id
        JOIN opt_products p ON o.product_id = p.product_id
        WHERE c.status = 'active'
    ) AS client_orders
    GROUP BY client_orders.product_category
    ORDER BY order_count DESC
) AS pcc
WHERE pcc.order_count = (
    SELECT MAX(pc.order_count)
    FROM (
        SELECT 
            COUNT(client_orders.client_name) AS order_count
        FROM (
            SELECT 
                CONCAT(c.name, ' ', c.surname) AS client_name, 
                c.status, 
                p.product_name, 
                p.product_category, 
                o.order_date
            FROM opt_orders o
            JOIN opt_clients c ON o.client_id = c.id
            JOIN opt_products p ON o.product_id = p.product_id
            WHERE c.status = 'active'
        ) AS client_orders
        GROUP BY client_orders.product_category
    ) AS pc
)
OR pcc.order_count = (
    SELECT MIN(pc.order_count)
    FROM (
        SELECT 
            COUNT(client_orders.client_name) AS order_count
        FROM (
            SELECT 
                CONCAT(c.name, ' ', c.surname) AS client_name, 
                c.status, 
                p.product_name, 
                p.product_category, 
                o.order_date
            FROM opt_orders o
            JOIN opt_clients c ON o.client_id = c.id
            JOIN opt_products p ON o.product_id = p.product_id
            WHERE c.status = 'active'
        ) AS client_orders
        GROUP BY client_orders.product_category
    ) AS pc
);

/* -> Filter: ((pcc.order_count = (select #4)) or (pcc.order_count = (select #7)))  (cost=0.592..61315 rows=103549) (actual time=19164..28167 rows=2 loops=1)
    -> Table scan on pcc  (cost=2.5..2.5 rows=0) (actual time=8837..8837 rows=5 loops=1)
        -> Materialize  (cost=0..0 rows=0) (actual time=8837..8837 rows=5 loops=1)
            -> Sort: order_count DESC  (actual time=8837..8837 rows=5 loops=1)
                -> Table scan on <temporary>  (actual time=8837..8837 rows=5 loops=1)
                    -> Aggregate using temporary table  (actual time=8837..8837 rows=5 loops=1)
                        -> Nested loop inner join  (cost=393290 rows=544997) (actual time=2.88..7713 rows=499366 loops=1)
                            -> Nested loop inner join  (cost=202541 rows=544997) (actual time=2.85..5096 rows=499366 loops=1)
                                -> Filter: (c.`status` = 'active')  (cost=11792 rows=53157) (actual time=2.33..426 rows=49951 loops=1)
                                    -> Table scan on c  (cost=11792 rows=106314) (actual time=2.32..391 rows=100000 loops=1)
                                -> Filter: (o.product_id is not null)  (cost=2.56 rows=10.3) (actual time=0.0811..0.092 rows=10 loops=49951)
                                    -> Index lookup on o using client_id (client_id=c.id)  (cost=2.56 rows=10.3) (actual time=0.0807..0.0902 rows=10 loops=49951)
                            -> Single-row index lookup on p using PRIMARY (product_id=o.product_id)  (cost=0.25 rows=1) (actual time=0.00488..0.00494 rows=1 loops=499366)
    -> Select #4 (subquery in condition; run only once)
        -> Aggregate: max(pc.order_count)  (cost=2.5..2.5 rows=1) (actual time=10326..10326 rows=1 loops=1)
            -> Table scan on pc  (cost=2.5..2.5 rows=0) (actual time=10326..10326 rows=5 loops=1)
                -> Materialize  (cost=0..0 rows=0) (actual time=10326..10326 rows=5 loops=1)
                    -> Table scan on <temporary>  (actual time=10326..10326 rows=5 loops=1)
                        -> Aggregate using temporary table  (actual time=10326..10326 rows=5 loops=1)
                            -> Nested loop inner join  (cost=393290 rows=544997) (actual time=1.03..8892 rows=499366 loops=1)
                                -> Nested loop inner join  (cost=202541 rows=544997) (actual time=1.02..5792 rows=499366 loops=1)
                                    -> Filter: (c.`status` = 'active')  (cost=11792 rows=53157) (actual time=0.613..494 rows=49951 loops=1)
                                        -> Table scan on c  (cost=11792 rows=106314) (actual time=0.61..452 rows=100000 loops=1)
                                    -> Filter: (o.product_id is not null)  (cost=2.56 rows=10.3) (actual time=0.0919..0.105 rows=10 loops=49951)
                                        -> Index lookup on o using client_id (client_id=c.id)  (cost=2.56 rows=10.3) (actual time=0.0914..0.103 rows=10 loops=49951)
                                -> Single-row index lookup on p using PRIMARY (product_id=o.product_id)  (cost=0.25 rows=1) (actual time=0.00583..0.00589 rows=1 loops=499366)
    -> Select #7 (subquery in condition; run only once)
        -> Aggregate: min(pc.order_count)  (cost=2.5..2.5 rows=1) (actual time=9003..9003 rows=1 loops=1)
            -> Table scan on pc  (cost=2.5..2.5 rows=0) (actual time=9003..9003 rows=5 loops=1)
                -> Materialize  (cost=0..0 rows=0) (actual time=9003..9003 rows=5 loops=1)
                    -> Table scan on <temporary>  (actual time=9003..9003 rows=5 loops=1)
                        -> Aggregate using temporary table  (actual time=9003..9003 rows=5 loops=1)
                            -> Nested loop inner join  (cost=393290 rows=544997) (actual time=1.35..7807 rows=499366 loops=1)
                                -> Nested loop inner join  (cost=202541 rows=544997) (actual time=1.33..5122 rows=499366 loops=1)
                                    -> Filter: (c.`status` = 'active')  (cost=11792 rows=53157) (actual time=0.671..447 rows=49951 loops=1)
                                        -> Table scan on c  (cost=11792 rows=106314) (actual time=0.666..409 rows=100000 loops=1)
                                    -> Filter: (o.product_id is not null)  (cost=2.56 rows=10.3) (actual time=0.0811..0.0921 rows=10 loops=49951)
                                        -> Index lookup on o using client_id (client_id=c.id)  (cost=2.56 rows=10.3) (actual time=0.0807..0.0904 rows=10 loops=49951)
                                -> Single-row index lookup on p using PRIMARY (product_id=o.product_id)  (cost=0.25 rows=1) (actual time=0.00502..0.00507 rows=1 loops=499366) */


CREATE INDEX client_status_index
ON opt_clients(status);

CREATE INDEX idx_opt_orders_client_id
ON opt_orders(client_id);

CREATE INDEX idx_opt_orders_product_id
ON opt_orders(product_id);

CREATE INDEX idx_opt_products_product_id
ON opt_products(product_id);


EXPLAIN ANALYZE
WITH actual_clients_info AS (
SELECT id, CONCAT(name, ' ', surname) AS client_name, status
FROM opt_clients oc
),
client_orders AS (
SELECT 
aci.client_name, 
aci.status, 
op.product_name, 
op.product_category, 
oo.order_date
FROM opt_orders oo
JOIN actual_clients_info aci ON oo.client_id = aci.id
JOIN opt_products op ON oo.product_id = op.product_id
WHERE aci.status = 'active'
),
product_category_count AS (
SELECT product_category, count(client_name) AS order_count FROM client_orders
GROUP BY product_category
ORDER BY order_count DESC)
SELECT concat(product_category, ': ', order_count) AS category_order_count
FROM product_category_count pcc
WHERE order_count = (
SELECT MAX(order_count) 
FROM product_category_count
) 
OR 
order_count = (
SELECT min(order_count) 
FROM product_category_count
);

/* -> Filter: ((pcc.order_count = (select #5)) or (pcc.order_count = (select #9)))  (cost=0.592..59720 rows=100856) (actual time=8276..8276 rows=2 loops=1)
    -> Table scan on pcc  (cost=2.5..2.5 rows=0) (actual time=8276..8276 rows=5 loops=1)
        -> Materialize CTE product_category_count if needed  (cost=0..0 rows=0) (actual time=8276..8276 rows=5 loops=1)
            -> Sort: order_count DESC  (actual time=8276..8276 rows=5 loops=1)
                -> Table scan on <temporary>  (actual time=8276..8276 rows=5 loops=1)
                    -> Aggregate using temporary table  (actual time=8276..8276 rows=5 loops=1)
                        -> Nested loop inner join  (cost=380034 rows=530824) (actual time=1.38..7158 rows=499366 loops=1)
                            -> Nested loop inner join  (cost=194246 rows=530824) (actual time=1.35..4801 rows=499366 loops=1)
                                -> Index lookup on oc using client_status_index (status='active'), with index condition: (oc.`status` = 'active')  (cost=8458 rows=53157) (actual time=0.885..682 rows=49951 loops=1)
                                -> Filter: (oo.product_id is not null)  (cost=2.5 rows=9.99) (actual time=0.0688..0.081 rows=10 loops=49951)
                                    -> Index lookup on oo using idx_opt_orders_client_id (client_id=oc.id)  (cost=2.5 rows=9.99) (actual time=0.0685..0.0793 rows=10 loops=49951)
                            -> Single-row index lookup on op using PRIMARY (product_id=oo.product_id)  (cost=0.25 rows=1) (actual time=0.00437..0.00442 rows=1 loops=499366)
    -> Select #5 (subquery in condition; run only once)
        -> Aggregate: max(product_category_count.order_count)  (cost=2.5..2.5 rows=1) (actual time=0.0142..0.0143 rows=1 loops=1)
            -> Table scan on product_category_count  (cost=2.5..2.5 rows=0) (actual time=0.0064..0.0076 rows=5 loops=1)
                -> Materialize CTE product_category_count if needed (query plan printed elsewhere)  (cost=0..0 rows=0) (never executed)
    -> Select #9 (subquery in condition; run only once)
        -> Aggregate: min(product_category_count.order_count)  (cost=2.5..2.5 rows=1) (actual time=0.0054..0.0055 rows=1 loops=1)
            -> Table scan on product_category_count  (cost=2.5..2.5 rows=0) (actual time=0.0024..0.0035 rows=5 loops=1)
                -> Materialize CTE product_category_count if needed (query plan printed elsewhere)  (cost=0..0 rows=0) (never executed) */