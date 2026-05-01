SELECT *
FROM samples.bakehouse.sales_transactions a
INNER JOIN samples.bakehouse.sales_customers b ON a.customerID = b.customerID
INNER JOIN samples.bakehouse.sales_franchises c ON a.franchiseID = c.franchiseID
INNER JOIN samples.bakehouse.media_gold_reviews_chunked d ON c.franchiseID = d.franchiseID
INNER JOIN samples.bakehouse.media_customer_reviews e ON e.franchiseID = c.franchiseID;
