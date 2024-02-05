WITH StoreTypeSales AS (
  SELECT
    dim_store_details.store_type,
    ROUND(CAST(SUM(orders_table.product_quantity * dim_products.product_price) AS numeric), 2) AS total_sales
  FROM
    orders_table
  INNER JOIN
    dim_store_details ON orders_table.store_code = dim_store_details.store_code
  INNER JOIN
    dim_products ON orders_table.product_code = dim_products.product_code
  GROUP BY
    dim_store_details.store_type
)
SELECT
  store_type,
  total_sales,
  ROUND(CAST((total_sales / SUM(total_sales) OVER ()) * 100 AS numeric), 2) AS percentage_total
FROM
  StoreTypeSales
ORDER BY
  total_sales DESC;
