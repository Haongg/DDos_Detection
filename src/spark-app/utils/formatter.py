from pyspark.sql.functions import col, to_timestamp, when, split

def clean_data(raw_df):
  request_path = split(col("request_url"), "\\?").getItem(0)

  url_type = (
    when(request_path.isin("/search", "/api/v1/search"), "search_url")
    .when(request_path.isin("/", "/home", "/about", "/contact"), "site_page_url")
    .when(request_path == "/cart", "cart_page_url")
    .when(request_path.startswith("/products/"), "product_detail_url")
    .when(request_path == "/products", "product_list_url")
    .when(request_path == "/blog", "blog_list_url")
    .when(request_path.startswith("/blog/"), "blog_detail_url")
    .when(request_path.startswith("/api/v1/login"), "api_login_url")
    .when(request_path.startswith("/api/v1/cart/items"), "api_cart_url")
    .when(request_path.startswith("/api/v1/users/"), "api_user_profile_url")
    .when(request_path == "/api/v1/report/export", "api_report_export_url")
    .when(request_path.isin("/.env", "/wp-admin", "/admin/config.php", "/etc/passwd"), "scan_url")
    .otherwise("other_url")
  )

  return raw_df.select(
    to_timestamp(col("timestamp") / 1000).alias("timestamp"),

    col("src_ip"),
    col("protocol"),
    col("method"),
    col("user_agent"),
    col("request_url"),
    request_path.alias("request_path"),
    url_type.alias("url_type"),

    col("status_code").cast("integer"),
    col("response_size").cast("integer"),

    when(col("status_code").between(200,299), "2xx")
    .when(col("status_code").between(400,499), "4xx")
    .when(col("status_code").between(500,599), "5xx")
    .otherwise("other").alias("status_category")
  )

 
