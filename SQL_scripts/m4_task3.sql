SELECT
  COUNT(dim_products.product_price) AS number_of_sales,
  SUM(orders_table.product_quantity) AS product_quantity_count,
  CASE
    WHEN dim_store_details.store_type = 'Web Portal' THEN 'Web'
    ELSE 'Offline'
  END AS location
FROM
  orders_table
INNER JOIN
  dim_store_details ON orders_table.store_code = dim_store_details.store_code
INNER JOIN 
  dim_products ON orders_table.product_code = dim_products.product_code
GROUP BY
  CASE
    WHEN dim_store_details.store_type = 'Web Portal' THEN 'Web'
    ELSE 'Offline'
  END
  ORDER BY number_of_sales;
