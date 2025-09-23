import logging
import re
from typing import NotRequired, TypedDict

import psycopg
from django.db import DatabaseError
from django.db.backends.utils import CursorWrapper
from psycopg import sql


class TableResult(TypedDict, total=False):
    success: bool
    error: NotRequired[str]
    message: NotRequired[str]


logger = logging.getLogger(__name__)


def create_transformation_functions(db_cursor: CursorWrapper) -> TableResult:
    """Create all PostgreSQL functions needed for preprocessing transformations."""
    try:
        # First Name normalization with nickname mapping
        db_cursor.execute(r"""
            CREATE OR REPLACE FUNCTION normalize_first_name(name TEXT)
            RETURNS TEXT AS $$
            BEGIN
                IF name IS NULL OR TRIM(name) = '' THEN
                    RETURN NULL;
                END IF;
                
                -- Lower case and trim
                name := LOWER(TRIM(name));
                -- Collapse whitespace
                name := REGEXP_REPLACE(name, ' +', ' ', 'g');
                -- Normalize accented characters 
                name := UNACCENT(name);
                -- Remove non-alpha characters
                name := REGEXP_REPLACE(name, '[^a-z\- ]', '', 'g');  
                name := TRIM(name);

                -- Handle common nicknames
                name := CASE name
                    WHEN 'bob' THEN 'robert'
                    WHEN 'bobby' THEN 'robert'
                    WHEN 'rob' THEN 'robert'
                    WHEN 'robby' THEN 'robert'
                    WHEN 'rick' THEN 'richard'
                    WHEN 'ricky' THEN 'richard'
                    WHEN 'rich' THEN 'richard'
                    WHEN 'dick' THEN 'richard'
                    WHEN 'bill' THEN 'william'
                    WHEN 'billy' THEN 'william'
                    WHEN 'will' THEN 'william'
                    WHEN 'willy' THEN 'william'
                    WHEN 'liz' THEN 'elizabeth'
                    WHEN 'beth' THEN 'elizabeth'
                    WHEN 'betsy' THEN 'elizabeth'
                    WHEN 'lisa' THEN 'elizabeth'
                    WHEN 'kate' THEN 'katherine'
                    WHEN 'katie' THEN 'katherine'
                    WHEN 'kathy' THEN 'katherine'
                    WHEN 'jen' THEN 'jennifer'
                    WHEN 'jenny' THEN 'jennifer'
                    WHEN 'mike' THEN 'michael'
                    WHEN 'mikey' THEN 'michael'
                    WHEN 'chris' THEN 'christopher'
                    WHEN 'topher' THEN 'christopher'
                    WHEN 'dan' THEN 'daniel'
                    WHEN 'danny' THEN 'daniel'
                    WHEN 'steve' THEN 'steven'
                    WHEN 'stevie' THEN 'steven'
                    WHEN 'jim' THEN 'james'
                    WHEN 'jimmy' THEN 'james'
                    WHEN 'jamie' THEN 'james'
                    WHEN 'johnny' THEN 'john'
                    WHEN 'johnnie' THEN 'john'
                    WHEN 'jack' THEN 'john'
                    WHEN 'andy' THEN 'andrew'
                    WHEN 'drew' THEN 'andrew'
                    WHEN 'pat' THEN 'patrick'
                    WHEN 'trish' THEN 'patricia'
                    WHEN 'tricia' THEN 'patricia'
                    WHEN 'sam' THEN 'samuel'
                    WHEN 'toni' THEN 'antonia'
                    WHEN 'tony' THEN 'anthony'
                    ELSE name
                END;
                
                RETURN NULLIF(name, '');
            END;
            $$ LANGUAGE plpgsql IMMUTABLE;
        """)

        # Last Name normalization
        db_cursor.execute(r"""
            CREATE OR REPLACE FUNCTION normalize_last_name(name TEXT)
            RETURNS TEXT AS $$
            BEGIN
                IF name IS NULL OR TRIM(name) = '' THEN
                    RETURN NULL;
                END IF;

                -- Lower case and trim
                name := LOWER(TRIM(name));
                -- Collapse whitespace
                name := REGEXP_REPLACE(name, ' +', ' ', 'g');
                -- Normalize accented characters 
                name := UNACCENT(name);
                -- Remove non-alpha characters
                name := REGEXP_REPLACE(name, '[^a-z\- ]', '', 'g');  
                name := TRIM(name);

                -- Handle common prefixes (longest first)
                IF name ~ '^de la ' THEN
                    name := 'de la ' || TRIM(SUBSTRING(name FROM 7));
                ELSIF name ~ '^del ' THEN
                    name := 'del ' || TRIM(SUBSTRING(name FROM 5));
                ELSIF name ~ '^van ' THEN
                    name := 'van ' || TRIM(SUBSTRING(name FROM 5));
                ELSIF name ~ '^von ' THEN
                    name := 'von ' || TRIM(SUBSTRING(name FROM 5));
                ELSIF name ~ '^mac ' THEN
                    name := 'mac ' || TRIM(SUBSTRING(name FROM 5));
                ELSIF name ~ '^de ' THEN
                    name := 'de ' || TRIM(SUBSTRING(name FROM 4));
                ELSIF name ~ '^da ' THEN
                    name := 'da ' || TRIM(SUBSTRING(name FROM 4));
                ELSIF name ~ '^di ' THEN
                    name := 'di ' || TRIM(SUBSTRING(name FROM 4));
                ELSIF name ~ '^du ' THEN
                    name := 'du ' || TRIM(SUBSTRING(name FROM 4));
                ELSIF name ~ '^la ' THEN
                    name := 'la ' || TRIM(SUBSTRING(name FROM 4));
                ELSIF name ~ '^le ' THEN
                    name := 'le ' || TRIM(SUBSTRING(name FROM 4));
                ELSIF name ~ '^mc ' THEN
                    name := 'mc ' || TRIM(SUBSTRING(name FROM 4));
                ELSIF name ~ '^st ' THEN
                    name := 'st ' || TRIM(SUBSTRING(name FROM 4));
                END IF; 

                RETURN NULLIF(name, '');
            END;
            $$ LANGUAGE plpgsql IMMUTABLE;
        """)

        # Sex normalization
        db_cursor.execute(r"""
            CREATE OR REPLACE FUNCTION normalize_sex(sex_value TEXT)
            RETURNS TEXT AS $$
            BEGIN
                IF sex_value IS NULL OR TRIM(sex_value) = '' THEN
                    RETURN NULL;
                END IF;

                -- Convert various representations to standard values
                sex_value := CASE sex_value
                    WHEN 'm' THEN 'M'
                    WHEN 'male' THEN 'M'
                    
                    WHEN 'f' THEN 'F'
                    WHEN 'female' THEN 'F'
                    
                    -- Anything else becomes NULL
                    ELSE NULL
                END;

                RETURN sex_value;
            END;
            $$ LANGUAGE plpgsql IMMUTABLE;
        """)

        # Race normalization
        db_cursor.execute(r"""
            CREATE OR REPLACE FUNCTION normalize_race(race TEXT)
            RETURNS TEXT AS $$
            BEGIN
                IF race IS NULL OR TRIM(race) = '' THEN
                    RETURN NULL;
                END IF;

                -- Normalize to lowercase and trim
                race := LOWER(TRIM(race));

                -- Convert various representations to standard values or NULL
                race := CASE race
                    -- Convert unknown/invalid to NULL
                    WHEN 'unknown' THEN NULL
                    WHEN 'asked but unknown' THEN NULL
                END;

                RETURN race;
            END;
            $$ LANGUAGE plpgsql IMMUTABLE;
        """)

        # Birth_date normalization
        db_cursor.execute(r"""
            CREATE OR REPLACE FUNCTION normalize_birth_date(birth_date TEXT)
            RETURNS TEXT AS $$
            BEGIN
                IF birth_date IS NULL OR TRIM(birth_date) = '' THEN
                    RETURN NULL;
                END IF;
                
                -- Trim whitespace
                birth_date := TRIM(birth_date);
                
                -- Convert various representations to NULL for invalid/unknown dates
                birth_date := CASE birth_date
                    WHEN 'unknown' THEN NULL
                    WHEN 'unk' THEN NULL
                    WHEN 'n/a' THEN NULL
                    WHEN 'na' THEN NULL
                    WHEN '00/00/0000' THEN NULL
                    WHEN '0000-00-00' THEN NULL
                    WHEN '1900-01-01' THEN NULL  -- Often used as placeholder
                    ELSE birth_date
                END;
                
                -- Validate date format (YYYY-MM-DD)
                IF birth_date IS NOT NULL AND birth_date !~ '^\d{4}-\d{2}-\d{2}$' THEN
                    RETURN NULL;
                END IF;
                
                RETURN birth_date;
            END;
            $$ LANGUAGE plpgsql IMMUTABLE;
        """)

        # Death date normalization
        db_cursor.execute(r"""
            CREATE OR REPLACE FUNCTION normalize_death_date(death_date TEXT)
            RETURNS TEXT AS $$
            BEGIN

                -- Handle NULL or empty strings
                IF death_date IS NULL OR TRIM(death_date) = '' THEN
                    RETURN NULL;
                END IF;

                -- Trim whitespace
                death_date := TRIM(death_date);

                -- Convert various representations to NULL for invalid/unknown dates
                death_date := CASE death_date
                    WHEN 'unknown' THEN NULL
                    WHEN 'unk' THEN NULL
                    WHEN 'n/a' THEN NULL
                    WHEN 'na' THEN NULL
                    WHEN '00/00/0000' THEN NULL
                    WHEN '0000-00-00' THEN NULL
                    WHEN '1900-01-01' THEN NULL  -- Often used as placeholder
                    ELSE death_date
                END;

                -- Validate date format (YYYY-MM-DD)
                IF death_date IS NOT NULL AND death_date !~ '^\d{4}-\d{2}-\d{2}$' THEN
                    RETURN NULL;
                END IF;

                RETURN death_date;
            END;
            $$ LANGUAGE plpgsql IMMUTABLE;
        """)

        # SSN Normalization
        db_cursor.execute(r"""  
            CREATE OR REPLACE FUNCTION normalize_ssn(ssn TEXT)
            RETURNS TEXT AS $$
            BEGIN
                IF ssn IS NULL OR TRIM(ssn) = '' THEN
                    RETURN NULL;
                END IF;
                
                -- Convert to string and strip whitespace
                ssn := TRIM(ssn);
                
                -- Remove trailing ".0" if present
                IF ssn LIKE '%.0' THEN
                    ssn := LEFT(ssn, LENGTH(ssn) - 2);
                END IF;
                
                -- Remove non-digit characters
                ssn := REGEXP_REPLACE(ssn, '[^\d]', '', 'g');
                
                -- Keep only if exactly 9 digits
                IF LENGTH(ssn) = 9 THEN
                    RETURN ssn;
                ELSE
                    RETURN NULL;
                END IF;
            END;
            $$ LANGUAGE plpgsql IMMUTABLE;
        """)

        # Address normalization
        db_cursor.execute(r"""
            CREATE OR REPLACE FUNCTION normalize_address(addr TEXT)
            RETURNS TEXT AS $$
            BEGIN
                IF addr IS NULL OR TRIM(addr) = '' THEN
                    RETURN NULL;
                END IF;

                addr := LOWER(TRIM(addr));

                -- Remove punctuation
                addr := REGEXP_REPLACE(addr, '[^\w\s]', '', 'g');

                -- Normalize PO Box patterns
                addr := REGEXP_REPLACE(addr, '\b(post\s+office|p[\s\.]?o[\s\.]?)\s*box\b', 'po box', 'g');
                
                -- Street suffixes
                addr := REGEXP_REPLACE(addr, '\bstreet\b', 'st', 'g');
                addr := REGEXP_REPLACE(addr, '\bavenue\b', 'ave', 'g');
                addr := REGEXP_REPLACE(addr, '\broad\b', 'rd', 'g');
                addr := REGEXP_REPLACE(addr, '\bdrive\b', 'dr', 'g');
                addr := REGEXP_REPLACE(addr, '\bboulevard\b', 'blvd', 'g');
                addr := REGEXP_REPLACE(addr, '\blane\b', 'ln', 'g');
                addr := REGEXP_REPLACE(addr, '\btrail\b', 'trl', 'g');
                addr := REGEXP_REPLACE(addr, '\bplace\b', 'pl', 'g');
                addr := REGEXP_REPLACE(addr, '\bsquare\b', 'sq', 'g');
                addr := REGEXP_REPLACE(addr, '\bcourt\b', 'ct', 'g');

                -- Unit descriptors
                addr := REGEXP_REPLACE(addr, '\bapartment\b', 'apt', 'g');
                addr := REGEXP_REPLACE(addr, '\bsuite\b', 'ste', 'g');
                addr := REGEXP_REPLACE(addr, '\bunit\b', 'unit', 'g');
                addr := REGEXP_REPLACE(addr, '\bbuilding\b', 'bldg', 'g');
                addr := REGEXP_REPLACE(addr, '\broom\b', 'rm', 'g');
                addr := REGEXP_REPLACE(addr, '\bapt\b', 'apt', 'g');

                -- Directions
                addr := REGEXP_REPLACE(addr, '\bnorth\b', 'n', 'g');
                addr := REGEXP_REPLACE(addr, '\bsouth\b', 's', 'g');
                addr := REGEXP_REPLACE(addr, '\beast\b', 'e', 'g');
                addr := REGEXP_REPLACE(addr, '\bwest\b', 'w', 'g');

                -- Remove stop words
                addr := REGEXP_REPLACE(addr, '\b(the|at|and|of|for)\b', '', 'g');

                -- Collapse multiple spaces
                addr := REGEXP_REPLACE(addr, '\s+', ' ', 'g');
                addr := TRIM(addr);

                RETURN NULLIF(addr, '');
            END;
            $$ LANGUAGE plpgsql IMMUTABLE;
        """)

        # City normalization
        db_cursor.execute(r"""
            CREATE OR REPLACE FUNCTION normalize_city(city TEXT)
            RETURNS TEXT AS $$
            BEGIN
                IF city IS NULL OR TRIM(city) = '' THEN
                    RETURN NULL;
                END IF;

                city := LOWER(TRIM(city));
                city := REGEXP_REPLACE(city, '[^\w\s]', '', 'g');
                city := REGEXP_REPLACE(city, '\s+', ' ', 'g');
                city := TRIM(city);

                -- Handle common aliases
                city := CASE city
                    WHEN 'nyc' THEN 'new york'
                    WHEN 'n y c' THEN 'new york'  -- This handles 'n.y.c.' after punctuation removal
                    WHEN 'sf' THEN 'san francisco'
                    WHEN 'la' THEN 'los angeles'
                    ELSE city
                END;

                -- Remove junk values
                IF city IN ('unknown', 'n/a', 'null', 'nan', '') THEN
                    RETURN NULL;
                END IF;

                RETURN NULLIF(city, '');
            END;
            $$ LANGUAGE plpgsql IMMUTABLE;
        """)

        # State normalization
        db_cursor.execute(r"""
            CREATE OR REPLACE FUNCTION normalize_state(state TEXT)
            RETURNS TEXT AS $$
            BEGIN
                IF state IS NULL OR TRIM(state) = '' THEN
                    RETURN NULL;
                END IF;

                -- Clean: uppercase and strip whitespace (matches Python)
                state := UPPER(TRIM(state));
        
                -- Check if it's a valid state/territory code
                IF state IN (
                    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI',
                    'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI',
                    'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC',
                    'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT',
                    'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'AS', 'GU', 'MP', 'PR', 'VI',
                    'AB', 'BC', 'MB', 'NB', 'NL', 'NS', 'ON', 'PE', 'QC', 'SK',
                    'NT', 'NU', 'YT'
                ) THEN
                    RETURN state;
                ELSE
                    RETURN NULL;
                END IF;
            END;
            $$ LANGUAGE plpgsql IMMUTABLE;
        """)

        # ZIP code normalization
        # Cleans a ZIP code column by:
        # 1. Converting to string
        # 2. Removing trailing '.0'
        # 3. Removing all non-digit characters
        # 4. Replacing junk values
        # 5. Keeping only 5- or 9-digit ZIPs
        # 6. Optionally truncating to 5 digits
        db_cursor.execute(r"""
            CREATE OR REPLACE FUNCTION normalize_zip(zip TEXT)
            RETURNS TEXT AS $$
            BEGIN
                IF zip IS NULL THEN
                    RETURN NULL;
                END IF;

                -- Remove .0 suffix irst, then all non-digits
                zip := TRIM(zip);
                zip := REGEXP_REPLACE(zip, '\.0$', '');
                zip := REGEXP_REPLACE(zip, '[^\d]', '', 'g');

                -- Remove known junk values
                IF zip IN ('00000', '99999', '12345') THEN
                    RETURN NULL;
                END IF;

                -- Keep only 5 or 9 digit ZIPs, truncate 9-digit to 5
                IF LENGTH(zip) = 5 THEN
                    RETURN zip;
                ELSIF
                    LENGTH(zip) = 9 THEN
                    RETURN SUBSTRING(zip, 1, 5);
                ELSE
                    RETURN NULL;
                END IF;
            END;
            $$ LANGUAGE plpgsql IMMUTABLE;
        """)

        # Phone normalization
        db_cursor.execute(r"""
            CREATE OR REPLACE FUNCTION normalize_phone(phone TEXT)
            RETURNS TEXT AS $$
            BEGIN
                IF
                    phone IS NULL THEN
                    RETURN NULL;
                END IF;

                -- Remove .0 suffix first, then all non-digits
                phone := TRIM(phone);
                phone := REGEXP_REPLACE(phone, '\.0$', '');
                phone := REGEXP_REPLACE(phone, '[^\d]', '', 'g');

                -- Remove known junk/placeholder numbers
                IF phone IN ('0000000000', '1234567890', '1111111111') THEN
                    RETURN NULL;
                END IF;

                -- Remove numbers with 7+ consecutive identical digits
                IF phone ~ '(.)\1{6,}' THEN
                    RETURN NULL;
                END IF;

                -- Keep only 10-digit phone numbers
                IF LENGTH(phone) = 10 THEN
                    RETURN phone;
                ELSE
                    RETURN NULL;
                END IF;
            END;
            $$ LANGUAGE plpgsql IMMUTABLE;
        """)
        return TableResult(success=True, message="Transformations successfully created")
    except psycopg.ProgrammingError as e:
        error_msg = f"Failed to create PostgreSQL transformation functions - SQL syntax error. Check function definitions and SQL syntax: {e}"
        logger.error(error_msg)
        return TableResult(success=False, message=error_msg)
    except psycopg.OperationalError as e:
        error_msg = f"Failed to create PostgreSQL transformation functions - database connection or operational error. Check database connectivity and permissions: {e}"
        logger.error(error_msg)
        return TableResult(success=False, message=error_msg)
    except DatabaseError as e:
        error_msg = f"Failed to create PostgreSQL transformation functions - database error. This may indicate insufficient privileges to create functions: {e}"
        logger.error(error_msg)
        return TableResult(success=False, message=error_msg)
    except Exception as e:
        error_msg = f"Failed to create PostgreSQL transformation functions - unexpected error during function creation: {e}"
        logger.error(error_msg)
        return TableResult(success=False, message=error_msg)


def transform_all_columns(db_cursor: CursorWrapper, temp_table: str) -> TableResult:
    """Apply all transformations in a single UPDATE statement."""
    # Validate first to guard again SQL injection
    # Only allow alphanumeric, underscoes
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]{0,62}$", temp_table):
        error_msg = f"Invalid table name: {temp_table}"
        logger.error(error_msg)
        return {"success": False, "error": error_msg}

    try:
        # TODO - this is currently 1 step. For speed. Do we want more information on where things fail?
        transform_sql = sql.SQL("""
            UPDATE {table_name} SET
                first_name = normalize_first_name(first_name),
                last_name = normalize_last_name(last_name),
                sex = normalize_sex(sex),
                race = normalize_race(race),
                birth_date = normalize_birth_date(birth_date),
                death_date = normalize_death_date(death_date),
                social_security_number = normalize_ssn(social_security_number),
                address = normalize_address(address),
                city = normalize_city(city),
                state = normalize_state(state),
                zip_code = normalize_zip(zip_code),
                phone = normalize_phone(phone)            
        """).format(table_name=sql.Identifier(temp_table))
        db_cursor.execute(transform_sql)
        return TableResult(success=True, message="Transformations successfully applied")

    except psycopg.ProgrammingError as e:
        error_msg = f"Failed to apply data transformations to table '{temp_table}' - SQL syntax error: {e}"
        logger.error(error_msg)
        return TableResult(success=False, message=error_msg)
    except psycopg.OperationalError as e:
        error_msg = f"Failed to apply data transformations to table '{temp_table}' - database connection or operational error. Check database connectivity: {e}"
        logger.error(error_msg)
        return TableResult(success=False, message=error_msg)
    except DatabaseError as e:
        error_msg = f"Failed to apply data transformations to table '{temp_table}' - database error. Ensure transformation functions exist and table is accessible: {e}"
        logger.error(error_msg)
        return TableResult(success=False, message=error_msg)
    except Exception as e:
        error_msg = f"Failed to apply data transformations to table '{temp_table}' - unexpected error during column transformation: {e}"
        logger.error(error_msg)
        return TableResult(success=False, message=error_msg)


def remove_invalid_and_dedupe(db_cursor: CursorWrapper, temp_table: str) -> TableResult:
    """Remove invalid records and duplicates."""
    # Validate table name
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]{0,62}$", temp_table):
        error_msg = f"Invalid table name: {temp_table}"
        logger.error(error_msg)
        return TableResult(success=False, message=error_msg)

    try:
        # Remove records with null source_person_id or '' after trimming
        delete_source_person_id_nulls_sql = sql.SQL("""
            DELETE FROM {table_name}
            WHERE source_person_id IS NULL
                OR TRIM (source_person_id) = ''
        """).format(table_name=sql.Identifier(temp_table))
        db_cursor.execute(delete_source_person_id_nulls_sql)

        # Remove records with null datasource or '' after trimming
        delete_data_source_nulls_sql = sql.SQL("""
            DELETE FROM {table_name}
            WHERE data_source IS NULL
                OR TRIM (data_source) = ''
        """).format(table_name=sql.Identifier(temp_table))
        db_cursor.execute(delete_data_source_nulls_sql)

        # Get count after removing nulls
        count_sql = sql.SQL("SELECT COUNT(*) FROM {table_name}").format(
            table_name=sql.Identifier(temp_table)
        )
        db_cursor.execute(count_sql)
        current_count = db_cursor.fetchone()[0]

        if current_count == 0:
            return TableResult(
                success=False,
                message=f"Table '{temp_table}' is empty after removing null source_person_id and data_source. Please check your data.",
            )

        # Remove duplicates (keeping first occurrence)
        delete_dupes_sql = sql.SQL("""
            DELETE FROM {table_name} a
                USING {table_name} b
                WHERE a.ctid > b.ctid
                AND COALESCE (a.source_person_id, '') = COALESCE (b.source_person_id, '')
                AND COALESCE (a.first_name, '') = COALESCE (b.first_name, '')
                AND COALESCE (a.last_name, '') = COALESCE (b.last_name, '')
                AND COALESCE (a.birth_date, '') = COALESCE (b.birth_date, '')
                AND COALESCE (a.social_security_number, '') = COALESCE (b.social_security_number, '')
        """).format(table_name=sql.Identifier(temp_table))
        db_cursor.execute(delete_dupes_sql)

        # get final count
        db_cursor.execute(count_sql)
        final_count = db_cursor.fetchone()[0]
        return TableResult(
            success=True,
            message=f"Removed {final_count} records from table '{temp_table}'",
        )

    except psycopg.Error as e:
        error_msg = f"Failed to clean up records from table '{temp_table} - {e}"
        logger.error(error_msg)
        return TableResult(success=False, error=str(error_msg))
    except DatabaseError as e:
        logger.error(
            f"Failed to cleanup records from table '{temp_table}' - database error. This may indicate table lock issues or insufficient permissions for DELETE operations: {e}"
        )
        return TableResult(success=False, error=str(e))
    except Exception as e:
        logger.error(
            f"Failed to cleanup records from table '{temp_table}' - unexpected error during record deletion and deduplication: {e}"
        )
        return TableResult(success=False, error=str(e))
