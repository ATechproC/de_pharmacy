from airflow.sdk import task

@task
def create_tables_snow():

    import pandas as pd
    from sqlalchemy import create_engine, inspect, text
    from urllib.parse import quote_plus
    import snowflake.connector
    import os
    import re

    # ============================================================
    # MYSQL CONFIG
    # ============================================================

    MYSQL_DB = {
        "user": "root",
        "password": "Anass2004mysql",
        "host": "host.docker.internal",
        "port": 3306,
        "database": "pharmacy_db"
    }


    # ============================================================
    # SNOWFLAKE CONFIG
    # ============================================================

    SNOW_CONF = {
        "account": "JUJSNUQ-RU38005",
        "user": "ATechproC",
        "password": "Anass2004snowflake",
        "database": "pharmacy_db",
        "schema": "common",
        "warehouse": "COMPUTE_WH",
        "role": "ACCOUNTADMIN"
    }

    # ============================================================
    # MYSQL CONNECTION
    # ============================================================

    mysql_password = quote_plus(MYSQL_DB["password"])

    mysql_url = (
        f"mysql+pymysql://"
        f"{MYSQL_DB['user']}:{mysql_password}@"
        f"{MYSQL_DB['host']}:{MYSQL_DB['port']}/"
        f"{MYSQL_DB['database']}"
    )

    mysql_engine = create_engine(mysql_url)


    # ============================================================
    # TEST MYSQL
    # ============================================================

    try:
        with mysql_engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        print("✅ MySQL Connection Successful")

    except Exception as e:
        print(f"❌ MySQL Connection Error: {e}")


    # ============================================================
    # SNOWFLAKE CONNECTION
    # ============================================================

    try:

        snow_conn = snowflake.connector.connect(
            account=SNOW_CONF["account"],
            user=SNOW_CONF["user"],
            password=SNOW_CONF["password"],
            database=SNOW_CONF["database"],
            schema=SNOW_CONF["schema"],
            warehouse=SNOW_CONF["warehouse"],
            role=SNOW_CONF["role"]
        )

        snow_cursor = snow_conn.cursor()

        snow_cursor.execute("""
            SELECT
                CURRENT_USER(),
                CURRENT_DATABASE(),
                CURRENT_SCHEMA(),
                CURRENT_WAREHOUSE(),
                CURRENT_ROLE()
        """)

        result = snow_cursor.fetchone()

        print("✅ Snowflake Connection Successful")
        print("User:", result[0])
        print("Database:", result[1])
        print("Schema:", result[2])
        print("Warehouse:", result[3])
        print("Role:", result[4])

    except Exception as e:
        print(f"❌ Snowflake Connection Error: {e}")


    # ============================================================
    # CONFIGURATION
    # ============================================================

    # Local directory where CSV files will be stored
    EXPORT_DIR = "mysql_exports"

    # Snowflake stage
    SNOWFLAKE_STAGE = "@PHARMACY_DB.COMMON.CSV_STAGE"


    # ============================================================
    # CREATE EXPORT DIRECTORY
    # ============================================================

    os.makedirs(
        EXPORT_DIR,
        exist_ok=True
    )


    # ============================================================
    # FUNCTION: GET NEXT FILE NUMBER
    # ============================================================

    def get_next_file_number(table):

        """
        Determines the next CSV number for a table.

        Example:

            mysql_exports/
            └── customers/
                ├── customers1.csv
                ├── customers2.csv
                └── customers3.csv

        Returns:

            4
        """

        # --------------------------------------------------------
        # Table directory
        # --------------------------------------------------------

        table_dir = os.path.join(
            EXPORT_DIR,
            table
        )

        # Create directory if it doesn't exist
        os.makedirs(
            table_dir,
            exist_ok=True
        )

        # --------------------------------------------------------
        # Pattern
        #
        # customers1.csv
        # customers2.csv
        # customers10.csv
        # --------------------------------------------------------

        pattern = rf"^{re.escape(table)}(\d+)\.csv$"

        numbers = []

        # --------------------------------------------------------
        # Look at existing files
        # --------------------------------------------------------

        for filename in os.listdir(table_dir):

            match = re.match(
                pattern,
                filename
            )

            if match:

                number = int(
                    match.group(1)
                )

                numbers.append(
                    number
                )

        # --------------------------------------------------------
        # No files yet
        # --------------------------------------------------------

        if not numbers:

            return 1

        return max(numbers) + 1

    inspector = inspect(mysql_engine)

    mysql_tables = inspector.get_table_names()


    # ============================================================
    # PROCESS MYSQL TABLES
    # ============================================================

    for table in mysql_tables:

        try:

            print(
                "\n"
                + "=" * 60
            )

            print(
                f"PROCESSING TABLE: {table}"
            )

            print(
                "=" * 60
            )


            # ====================================================
            # EXTRACT TABLE FROM MYSQL
            # ====================================================

            print(
                f"Extracting {table} from MySQL..."
            )

            df = pd.read_sql_table(
                table,
                mysql_engine
            )


            # ====================================================
            # CHECK EMPTY TABLE
            # ====================================================

            if df.empty:

                print(
                    f"{table} is empty. Skipping."
                )

                continue


            print(
                f"Rows extracted: {len(df)}"
            )


            # ====================================================
            # NORMALIZE COLUMN NAMES
            # ====================================================

            df.columns = [
                str(column)
                .strip()
                .upper()
                for column in df.columns
            ]


            # ====================================================
            # HANDLE DATE / DATETIME COLUMNS
            # ====================================================

            for column in df.columns:

                if (
                    "DATE" in column.upper()
                    or
                    "TIME" in column.upper()
                ):

                    try:

                        df[column] = pd.to_datetime(
                            df[column],
                            errors="coerce"
                        )

                    except Exception:

                        pass


            # ====================================================
            # CREATE TABLE DIRECTORY
            # ====================================================

            table_dir = os.path.join(
                EXPORT_DIR,
                table
            )

            os.makedirs(
                table_dir,
                exist_ok=True
            )


            # ====================================================
            # GET NEXT FILE NUMBER
            # ====================================================

            file_number = get_next_file_number(
                table
            )

            print(
                f"Next file number: {file_number}"
            )


            # ====================================================
            # CREATE FILE NAME
            # ====================================================

            filename = (
                f"{table}{file_number}.csv"
            )


            # ====================================================
            # CREATE LOCAL FILE PATH
            # ====================================================

            file_path = os.path.abspath(
                os.path.join(
                    table_dir,
                    filename
                )
            )


            # ====================================================
            # WRITE CSV
            # ====================================================

            df.to_csv(
                file_path,
                index=False,
                na_rep=""
            )

            print(
                f"Created CSV:"
            )

            print(
                file_path
            )


            # ====================================================
            # CREATE WINDOWS FILE URI
            # ====================================================

            file_uri = (
                "file://"
                + file_path.replace(
                    "\\",
                    "/"
                )
            )


            # ====================================================
            # SNOWFLAKE STAGE PATH
            # ====================================================

            stage_path = (
                f"{SNOWFLAKE_STAGE}/{table}/"
            )


            print(
                f"Uploading {filename}"
            )

            print(
                f"Stage: {stage_path}"
            )


            # ====================================================
            # PUT FILE INTO SNOWFLAKE STAGE
            # ====================================================

            snow_cursor.execute(
                f"""
                PUT '{file_uri}'
                {stage_path}
                AUTO_COMPRESS=TRUE
                OVERWRITE=FALSE
                """
            )


            # ====================================================
            # GET PUT RESULT
            # ====================================================

            result = snow_cursor.fetchall()


            for row in result:

                print(
                    row
                )


            # ====================================================
            # SUCCESS
            # ====================================================

            print(
                f"✅ {filename} successfully uploaded"
            )

            print(
                f"   Local : {file_path}"
            )

            print(
                f"   Stage : {stage_path}"
            )


        except Exception as e:

            print(
                f"\n❌ Failed to migrate {table}"
            )

            print(
                f"Error: {e}"
            )


    # ============================================================
    # COMPLETE
    # ============================================================

    print(
        "\n"
        + "=" * 60
    )

    print(
        "ALL TABLES PROCESSED"
    )

    print(
        "=" * 60
    )

create_tables_snow()