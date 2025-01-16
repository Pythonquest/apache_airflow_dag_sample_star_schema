from dbConnect import my_connection
from MySQLdb import Error


# These functions define airflow tasks for intermediate models in a star schema.
def _create_int_accounts():
    # intermediate model for account dimension
    try:
        c, conn = my_connection()
        c.execute("use radancy1;")
        sql_text = """
            create table int_accounts as (
                with 
                    stgData as (
                        select * from stg_customers                        
                    ),

                    qryFinal as 
                    (
                        select distinct
                            account_number,
                            account_name,
                            account_status
                        from stgData
                    )

                select * from qryFinal
                );                
            """
        c.execute(sql_text)
        conn.commit()
    # Trap and report any error in the process.
    except Error as e:
        print("Error while processing:", e)

def _create_int_ad_campaigns():
    # intermediate model for ad campaign dimension
    try:
        c, conn = my_connection()
        c.execute("use radancy1;")
        sql_text = """
            create table int_ad_campaigns as (
                with 
                    stgData as (
                        select * from stg_customers                        
                    ),

                    qryFinal as 
                    (
                        select distinct
                            campaign_name,
                            campaign_status,
                            ad_group_ID,
                            ad_group_name,
                            ad_group_status,
                            ad_id,
                            ad_description,
                            ad_distribution,
                            ad_status,
                            ad_title,
                            ad_type,
                            tracking_template,
                            custom_parameters,
                            final_mobile_url,
                            final_url,
                            tp_vs_other,
                            display_url,
                            final_app_url,
                            destination_url
                        from stgData
                    )

                select * from qryFinal
                );                
            """
        c.execute(sql_text)
        conn.commit()
    # Trap and report any error in the process.
    except Error as e:
        print("Error while processing:", e)

def _create_int_customer_devices():
    # intermediate model for customer device dimension
    try:
        c, conn = my_connection()
        c.execute("use radancy1;")
        sql_text = """
            create table int_customer_devices as (
                with 
                    stgData as (
                        select * from stg_customers                        
                    ),

                    qryFinal as 
                    (
                        select distinct
                            customer_ID,
                            device_type,
                            device_operating_system
                        from stgData
                    )

                select * from qryFinal
                );                
            """
        c.execute(sql_text)
        conn.commit()
    # Trap and report any error in the process.
    except Error as e:
        print("Error while processing:", e)

def _create_int_customers():
    # intermediate model for customer dimension
    try:
        c, conn = my_connection()
        c.execute("use radancy1;")
        sql_text = """
            create table int_customers as (
                with 
                    stgData as (
                        select * from stg_customers                        
                    ),

                    qryFinal as 
                    (
                        select distinct
                            customer_ID,
                            language,
                            trim(network) as network,
                            trim(currency_code) as currency_code
                        from stgData
                    )

                select * from qryFinal
                );                
            """
        c.execute(sql_text)
        conn.commit()
    # Trap and report any error in the process.
    except Error as e:
        print("Error while processing:", e)

def _create_int_fct_customer_activity():
    # intermediate model for customer activity fact table
    try:
        c, conn = my_connection()
        c.execute("use radancy1;")
        sql_text = """
            create table int_customers as (
                with 
                    stgData as (
                        select * from stg_customers                        
                    ),

                    qryFinal as 
                    (
                        select distinct
                            gregorian_date,
                            customer_ID,
                            ad_group_ID,
                            ad_ID,
                            sum(impressions) as total_impressions,
                            sum(clicks) as total_clicks,
                            sum(spend) as total_spent,
                            avg(average_position) as average_position,
                            sum(conversions) as total_conversions,
                            sum(assists) as total_assists
                        from stgData
                        group by
                            gregorian_date,
                            customer_ID,
                            ad_group_ID,
                            ad_ID
                    )

                select * from qryFinal
                );                
            """
        c.execute(sql_text)
        conn.commit()
    # Trap and report any error in the process.
    except Error as e:
        print("Error while processing:", e)
