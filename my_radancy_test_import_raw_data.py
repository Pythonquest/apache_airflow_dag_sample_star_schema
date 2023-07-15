from dbConnect import my_connection
from MySQLdb import Error
import pandas
    

def _import_raw_data():
    # Create a data frame to hold source data. Header and footer lines are excluded.
    customerdata = pandas.read_csv('/home/rfountain/customer_data.csv', delimiter=',', skiprows=9, skipfooter=2, skip_blank_lines=True)
    # Replace nan values with empty strings to facilitate the import into MySQL.
    reformatted_data = customerdata.fillna('')

    try:
        # Connect to MySQL server using credentials established in seperate connection file.
        c, conn = my_connection()

        # Create and activate database within server.
        c.execute("create database if not exists radancy1;")
        c.execute("use radancy1;")

        # Create a table to hold imported raw customer data.
        c.execute("drop table if exists raw_customer_data;")
        c.execute("""
            create table raw_customer_data(
                gregorian_date varchar(10),
                customer varchar(10),
                account_number varchar(8),
                account_name varchar(100),
                account_status varchar(10),
                campaign_name varchar(255),
                campaign_status varchar(20),
                ad_group_ID varchar(20),
                ad_group varchar(255),
                ad_group_status varchar(10),
                ad_id varchar(20),
                ad_description varchar(255),
                ad_distribution varchar(20),
                ad_status varchar(10),
                ad_title varchar(255),
                ad_type varchar(30),
                tracking_template varchar(255),
                custom_parameters varchar(255),
                final_mobile_url blob,
                final_url blob,
                tp_vs_other varchar(255),
                display_url blob,
                final_app_url varchar(255),
                destination_url varchar(255),
                device_type varchar(20),
                device_os varchar(20),
                delivered_match varchar(20),
                bid_match_type varchar(20),
                language varchar(20),
                network varchar(30),
                currency_code varchar(5),
                impressions varchar(10),
                clicks varchar(10),
                spend varchar(10),
                avg_position varchar(10),
                conversions varchar(10),
                assists varchar(10));
                """)

        # Import the customer data line by line to raw table.
        for i, row in reformatted_data.iterrows():
            # SQL statement that will populate the raw table according to the currently read line.
            sql = """
                insert into radancy1.raw_customer_data
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
            c.execute(sql, tuple(row))
            conn.commit()
            # Debug line
            #print("Inserted row ", i)

    # Trap and report any error in the process.
    except Error as e:
        print("Error while processing:", e)
