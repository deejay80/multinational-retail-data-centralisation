WITH StoreTypeSales AS (
  SELECT
    dim_store_details.store_type,
    ROUND(CAST(SUM(orders_table.product_quantity * dim_products.product_price) AS numeric), 2) AS total_sales,
    dim_store_details.country_code
  FROM
    orders_table
  INNER JOIN
    dim_store_details ON orders_table.store_code = dim_store_details.store_code
  INNER JOIN
    dim_products ON orders_table.product_code = dim_products.product_code
  WHERE
    dim_store_details.country_code = 'DE'
  GROUP BY
    dim_store_details.store_type, dim_store_details.country_code
)

SELECT
  total_sales,
  store_type,
  country_code
FROM
  StoreTypeSales
ORDER BY
  total_sales ;

