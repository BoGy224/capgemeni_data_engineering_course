#Task 1
WITH book_count(name,book_count) as (
SELECT authors.name as name,COUNT(*)
	as book_count from books JOIN authors on
	books.author_id = authors.author_id group by authors.author_id
	)
	
SELECT  * FROM book_count WHERE book_count > 3

#Task 2
WITH sub_books(title,author,genre,published_date) as (
SELECT books.title,authors.name,genres.genre_name,books.published_date FROM books
	JOIN authors ON books.author_id = authors.author_id
	JOIN genres ON books.genre_id = genres.genre_id
	WHERE books.title ~'the |The'
	)
	
SELECT * FROM sub_books;

#Task 3 
SELECT genres.genre_name,books.title,
	RANK () OVER (
	   PARTITION BY genres.genre_name ORDER BY
			price
	) as price_rank
FROM books JOIN genres ON 
	books.genre_id = genres.genre_id 


#Task 4
CREATE OR REPLACE PROCEDURE sp_bulk_update_book_prices_by_genre(
   p_genre_id int,
   p_percentage_change numeric(5,2)
)
LANGUAGE plpgsql    
AS $$DECLARE
    books_updated INT;
BEGIN
    -- Update the prices of books in the specified genre
    UPDATE books
    SET price = price * (1 + p_percentage_change / 100)
    WHERE genre_id = p_genre_id;

    -- Get the number of rows that were updated
    GET DIAGNOSTICS books_updated = ROW_COUNT;

    -- Output the number of updated books
    RAISE NOTICE 'Number of books updated: %', books_updated;
END;
$$;

SELECT price from books where genre_id = 3
CALL sp_bulk_update_book_prices_by_genre(3, 5.00)
SELECT price from books where genre_id = 3


# Task 5
CREATE OR REPLACE PROCEDURE sp_update_customer_join_date()
LANGUAGE plpgsql    
AS $$DECLARE
    customers_updated INT;
BEGIN
    -- Update the join_date of customers to the date of their first purchase if it's earlier
    UPDATE customers c
    SET join_date = fp.first_purchase_date
    FROM (
		SELECT customer_id, MIN(sale_date) AS first_purchase_date
        FROM sales
        GROUP BY customer_id
    ) fp
    WHERE c.customer_id = fp.customer_id
      AND fp.first_purchase_date < c.join_date;

    -- Get the number of rows that were updated
    GET DIAGNOSTICS customers_updated = ROW_COUNT;

    -- Output the number of updated customers
    RAISE NOTICE 'Number of customers updated: %', customers_updated;
END;
$$;


CALL sp_update_customer_join_date();
SELECT customer_id,join_date FROM customers

#Task 6
CREATE OR REPLACE FUNCTION fn_avg_price_by_genre(p_genre_id INTEGER)
RETURNS NUMERIC(10, 2)
LANGUAGE plpgsql
AS $$
DECLARE
    avg_price NUMERIC(10, 2);
BEGIN
    -- Calculate the average price of books within the specified genre
    SELECT ROUND(AVG(price), 2)
    INTO avg_price
    FROM books
    WHERE genre_id = p_genre_id;

    -- Return the calculated average price
    RETURN avg_price;
END;
$$;

SELECT fn_avg_price_by_genre(1);

# Task 7 
CREATE OR REPLACE FUNCTION fn_get_top_n_books_by_genre(p_genre_id INTEGER, p_top_n INTEGER)
RETURNS TABLE(book_id INTEGER, title VARCHAR, total_revenue NUMERIC(10, 2))
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT b.book_id,b.title,SUM(s.quantity * b.price) AS total_revenue
    FROM books b
    JOIN sales s ON b.book_id = s.book_id
    WHERE b.genre_id = p_genre_id
    GROUP BY b.book_id, b.title
    ORDER BY total_revenue DESC
    LIMIT p_top_n;
END;
$$;

SELECT * FROM fn_get_top_n_books_by_genre(1, 5);

# Task 8
CREATE OR REPLACE FUNCTION fn_log_sensitive_data_changes()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    -- Log changes to first_name
    IF NEW.first_name <> OLD.first_name THEN
        INSERT INTO CustomersLog (column_name, old_value, new_value, changed_by)
        VALUES ('first_name', OLD.first_name, NEW.first_name, CURRENT_USER);
    END IF;

    -- Log changes to last_name
    IF NEW.last_name <> OLD.last_name THEN
        INSERT INTO CustomersLog (column_name, old_value, new_value, changed_by)
        VALUES ('last_name', OLD.last_name, NEW.last_name, CURRENT_USER);
    END IF;

    -- Log changes to email_address
    IF NEW.email <> OLD.email THEN
        INSERT INTO CustomersLog (column_name, old_value, new_value, changed_by)
        VALUES ('email_address', OLD.email, NEW.email, CURRENT_USER);
    END IF;

    RETURN NEW;
END;
$$;

CREATE TABLE CustomersLog (
    log_id SERIAL PRIMARY KEY,
    column_name VARCHAR(50),
    old_value TEXT,
    new_value TEXT,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    changed_by VARCHAR(50)  -- Assuming you can track the user making the change
);

CREATE TRIGGER tr_log_sensitive_data_changes
AFTER UPDATE ON Customers
FOR EACH ROW
EXECUTE FUNCTION fn_log_sensitive_data_changes();

SELECT * FROM customers WHERE customer_id = 1;
UPDATE customers SET last_name = 'NewSmith' where customer_id = 1;
SELECT * FROM customerslog



# Task 9
CREATE OR REPLACE FUNCTION fn_adjust_book_price()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    total_quantity_sold INT;
    price_increase_threshold INT := 10; -- The threshold for increasing the price
BEGIN
    -- Calculate the total quantity sold for the book
    SELECT SUM(quantity)
    INTO total_quantity_sold
    FROM sales
    WHERE book_id = NEW.book_id;

    -- If the total quantity sold reaches or exceeds the threshold, increase the price by 10%
    IF total_quantity_sold >= price_increase_threshold THEN
        UPDATE books
        SET price = price * 1.10
        WHERE book_id = NEW.book_id;
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER tr_adjust_book_price
AFTER INSERT ON sales
FOR EACH ROW
EXECUTE FUNCTION fn_adjust_book_price();


# Task 10
CREATE TABLE SalesArchive AS TABLE sales WITH NO DATA;

CREATE OR REPLACE PROCEDURE sp_archive_old_sales(IN p_cutoff_date DATE)
LANGUAGE plpgsql
AS $$
DECLARE v_sale_id INT;
DECLARE v_book_id INT;
DECLARE v_sale_date DATE;
DECLARE v_customer_id INT;
DECLARE v_quantity INT;
    
DECLARE cur_sales CURSOR FOR 
SELECT sale_id,book_id,customer_id,quantity,sale_date FROM sales WHERE sale_date < p_cutoff_date;
    
BEGIN
    
    OPEN cur_sales;
    
    LOOP
        FETCH NEXT FROM cur_sales INTO v_sale_id,v_book_id,v_customer_id,v_quantity,v_sale_date;
        EXIT WHEN NOT FOUND;

        INSERT INTO SalesArchive (sale_id,book_id,customer_id,quantity,sale_date)
        VALUES (v_sale_id,v_book_id,v_customer_id,v_quantity,v_sale_date);
        
        DELETE FROM sales WHERE sale_id = v_sale_id;
    END LOOP;
    CLOSE cur_sales;
END$$

CALL sp_archive_old_sales('2023-01-01');