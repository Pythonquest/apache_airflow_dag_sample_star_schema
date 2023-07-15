from dbConnect import my_connection
from MySQLdb import Error


def _create_stg_customers():
    # create a staging model with only minor transformations.
    try:
        c, conn = my_connection()
        c.execute("use radancy1;")
        sql_text = """
            create table stg_customers as (
                with
                    raw_customers as (
                        select * from radancy1.raw_customer_data
                    ),

                    qryFinal as
                    (
                        select
                            cast(gregorian_date as date) as gregorian_date,
                            cast(customer as unsigned) as customer_ID,
                            trim(account_number) as account_number,
                            trim(account_name) as account_name,
                            trim(account_status) as account_status,
                            trim(campaign_name) as campaign_name,
                            trim(campaign_status) as campaign_status,
                            trim(ad_group_ID) as ad_group_ID,
                            trim(ad_group) as ad_group_name,
                            trim(ad_group_status) as ad_group_status,
                            trim(ad_id) as ad_ID,
                            trim(ad_description) as ad_description,
                            trim(ad_distribution) as ad_distribution,
                            trim(ad_status) as ad_status,
                            trim(ad_title) as ad_title,
                            trim(ad_type) as ad_type,
                            trim(tracking_template) as tracking_template,
                            trim(custom_parameters) as custom_parameters,
                            trim(final_mobile_url) as final_mobile_url,
                            trim(final_url) as final_url,
                            trim(tp_vs_other) as tp_vs_other,
                            trim(display_url) as display_url,
                            trim(final_app_url) as final_app_url,
                            trim(destination_url) as destination_url,
                            trim(device_type) as device_type,
                            trim(device_os) as device_operating_system,
                            trim(delivered_match) as delivered_match,
                            trim(bid_match_type) as bid_match_type,
                            trim(language) as language,
                            trim(network) as network,
                            trim(currency_code) as currency_code,
                            cast(trim(impressions) as unsigned) as impressions,
                            cast(trim(clicks) as unsigned) as clicks,
                            cast(trim(spend) as float) as spend,
                            cast(trim(avg_position) as float) as average_position,
                            cast(trim(conversions) as unsigned) as conversions,
                            cast(trim(assists) as unsigned) as assists
                        from raw_customers
                    )
                    
                    select * from qryFinal
                );
                """
        c.execute(sql_text)
        conn.commit()
    # Trap and report any error in the process.
    except Error as e:
        print("Error while processing:", e)
