# mysql_utils.py - Utility functions for MySQL database operations.

from typing import List, Tuple, Optional, Union, Any
import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv
import time
import threading

# Load environment variables from .env file
load_dotenv(override=True)

def _safe_int(value: Any, default: int = 0) -> int:
    """Safely convert a database value to int."""
    if value is None:
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default

def _safe_float(value: Any, default: float = 0.0) -> float:
    """Safely convert a database value to float."""
    if value is None:
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default

def get_db_connection() -> Any:
    """Create and return a new connection to AWS RDS MySQL."""
    max_retries = 3
    retry_delay_seconds = 2

    for attempt in range(1, max_retries + 1):
        try:
            connection = mysql.connector.connect(
                host=os.getenv("DB_HOST"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                database=os.getenv("DB_NAME"),
                port=int(os.getenv("DB_PORT", 3306)),
                connect_timeout=30  # 30-second timeout
            )
            print(f"MySQL connection established (Attempt {attempt}/{max_retries})")
            return connection
        except Error as e:
            print(f"MySQL connection failed (Attempt {attempt}/{max_retries}): {e}")
            if attempt < max_retries:
                print(f"Retrying in {retry_delay_seconds} seconds...")
                time.sleep(retry_delay_seconds)
            else:
                print("Max retries reached. Raising exception.")
                raise
    
    # This should never be reached, but satisfies the type checker
    raise RuntimeError("Failed to establish database connection")


def close_db_connection(cursor: Optional[Any], cnx: Optional[Any]) -> None:
    """Safely close MySQL cursor and connection."""
    if cursor:
        cursor.close()
    if cnx:
        cnx.close()


def get_all_tables() -> List[str]:
    """Fetch all table names from the MySQL database."""
    cnx, cursor = None, None
    try:
        cnx = get_db_connection()
        cursor = cnx.cursor()
        cursor.execute("SHOW TABLES")
        results = cursor.fetchall()
        return [str(table[0]) for table in results]  # (table_name,) -> table_name
    except Exception as e:
        print("Database connection error:", e)
        return []
    finally:
        close_db_connection(cursor, cnx)


def get_table_count(table_name: str) -> int:
    """Fetch row count for the selected table."""
    cnx, cursor = None, None
    try:
        cnx = get_db_connection()
        cursor = cnx.cursor()
        query = f"SELECT COUNT(*) FROM {table_name}"
        cursor.execute(query)
        result = cursor.fetchone()
        return _safe_int(result[0]) if result else 0  # (count,) -> count
    except Exception as e:
        print(f"Error fetching count for table '{table_name}':", e)
        return 0
    finally:
        close_db_connection(cursor, cnx)


def find_universities_with_faculties_working_keywords(keyword: str) -> List[Tuple[str, int]]:
    """Find top 5 universities with number of faculties working on the given keyword."""
    cnx, cursor = None, None
    try:
        cnx = get_db_connection()
        cursor = cnx.cursor()

        # Create a View to store the top universities with faculty count
        query_view = """CREATE OR REPLACE VIEW TOP_UNIVERSITIES AS
                        SELECT university.name, COUNT(distinct(faculty.id)) AS faculty_count
                        FROM university, faculty, faculty_keyword, keyword
                        WHERE university.id = faculty.university_id
                        AND faculty.id = faculty_keyword.faculty_id
                        AND faculty_keyword.keyword_id = keyword.id
                        AND keyword.name LIKE %s
                        GROUP BY university.name;"""
        cursor.execute(query_view, (f"%{keyword}%",))  # Secure way to pass parameters
        cnx.commit()

        # Query the view to fetch top universities with faculty count
        query = "SELECT * FROM TOP_UNIVERSITIES ORDER BY faculty_count DESC LIMIT 5"
        cursor.execute(query)
        
        results = cursor.fetchall()
        return [(str(row[0]), _safe_int(row[1])) for row in results]  # [(university, faculty_count), ...]
    except Exception as e:
        print("Error fetching universities:", e)
        return []
    finally:
        close_db_connection(cursor, cnx)


# For 1. Widget One: MongoDB Bar Chart (with MySQL option)
def find_most_popular_keywords_sql(year: int) -> List[Tuple[str, int]]:
    """Find top-10 most popular keywords among publications since 2015."""
    cnx, cursor = None, None
    try:
        cnx = get_db_connection()
        cursor = cnx.cursor()

        # Create indexes for optimized query performance
        index_definitions = {
            "idx_publication_year": "CREATE INDEX idx_publication_year ON publication(year);",
            "idx_pubkw_pubid_kwid": "CREATE INDEX idx_pubkw_pubid_kwid ON publication_keyword(publication_id, keyword_id);",
            "idx_pubkw_kwid": "CREATE INDEX idx_pubkw_kwid ON publication_keyword(keyword_id);",
            "idx_keyword_id": "CREATE INDEX idx_keyword_id ON keyword(id);"
        }

        for index_name, index_sql in index_definitions.items():
            cursor.execute("""
                SELECT COUNT(*) FROM INFORMATION_SCHEMA.STATISTICS
                WHERE table_schema = DATABASE()
                AND index_name = %s
            """, (index_name,))
            result = cursor.fetchone()
            exists = _safe_int(result[0]) if result else 0
            if not exists:
                try:
                    cursor.execute(index_sql)
                except Exception as index_error:
                    print(f"Index creation failed for {index_name}:", index_error)
        
        query = """SELECT keyword.name, COUNT(publication.id)
                   FROM keyword, publication_keyword, publication
                   WHERE keyword.id = publication_keyword.keyword_id
                   AND publication_keyword.publication_id = publication.id
                   AND publication.year >= %s
                   GROUP BY keyword_id ORDER BY COUNT(keyword_id) DESC LIMIT 10;"""
        cursor.execute(query, (year,))
        results = cursor.fetchall()
        return [(str(row[0]), _safe_int(row[1])) for row in results]  # [(keyword, count), ...]
    except Exception as e:
        print("Error fetching popular keywords:", e)
        return []
    finally:
        close_db_connection(cursor, cnx)
    

# For 2. Widget Two: MySQL Controller
def get_all_keywords() -> List[str]:
    """Fetch all keywords from the MySQL database."""
    cnx, cursor = None, None
    try:
        cnx = get_db_connection()
        cursor = cnx.cursor()
        cursor.execute("SELECT DISTINCT(name) FROM keyword")
        results = cursor.fetchall()
        return [str(keyword[0]) for keyword in results]  # (keyword,) -> keyword
    except Exception as e:
        print("Error fetching keywords:", e)
        return []
    finally:
        close_db_connection(cursor, cnx)


# For 3.1 Widget Three: MySQL Table
def find_faculty_relevant_to_keyword(keyword: str) -> List[Tuple[str, str, str]]:
    """Find faculty members relevant to the selected keyword."""
    cnx, cursor = None, None
    try:
        cnx = get_db_connection()
        cursor = cnx.cursor()

        # Prepared Statement: define the SQL statement with a placeholder
        # Benefit: avoid SQL injection and improve performance
        prepare_query = """PREPARE stmt FROM 
                           'SELECT faculty.id, faculty.name, university.name
                            FROM university, faculty, faculty_keyword, keyword
                            WHERE university.id = faculty.university_id
                            AND faculty.id = faculty_keyword.faculty_id
                            AND faculty_keyword.keyword_id = keyword.id
                            AND keyword.name = ? 
                            AND faculty_keyword.score >= 50
                            AND faculty.is_deleted = FALSE';"""
        # Prepare the statement
        cursor.execute(prepare_query)

        # Assign value to the parameter
        cursor.execute("SET @keyword = %s", (keyword,))

        # Execute the prepared statement using the variable
        cursor.execute("EXECUTE stmt USING @keyword;")

        # Fetch results
        results = cursor.fetchall()

        # Deallocate the prepared statement
        cursor.execute("DEALLOCATE PREPARE stmt;")

        return [(str(row[0]), str(row[1]), str(row[2])) for row in results]  # [(faculty_id, faculty_name, university_name), ...]
    except Exception as e:
        print("Error fetching faculty members:", e)
        return []
    finally:
        close_db_connection(cursor, cnx)


# For 3.1 Widget Three: MySQL Table - Count Faculty
def get_faculty_count() -> int:
    """Fetch the total number of faculty members from the MySQL database."""
    cnx, cursor = None, None
    try:
        cnx = get_db_connection()
        cursor = cnx.cursor()

        # Check if 'is_deleted' column exists (case-insensitive check)
        cursor.execute("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = DATABASE() 
            AND TABLE_NAME = 'faculty' 
            AND COLUMN_NAME = 'is_deleted'
        """)
        result = cursor.fetchone()
        column_exists = (_safe_int(result[0]) > 0) if result else False

        if not column_exists:
            try:
                cursor.execute("ALTER TABLE faculty ADD COLUMN is_deleted BOOLEAN DEFAULT FALSE")
                cnx.commit()
            except Exception as alter_error:
                print(f"Error adding column: {alter_error}")
                # Continue even if column creation fails

        cursor.execute("SELECT COUNT(*) FROM faculty WHERE is_deleted = FALSE")
        result = cursor.fetchone()
        return _safe_int(result[0]) if result else 0
    except Exception as e:
        print(f"Error fetching faculty count: {e}")
        return 0
    finally:
        close_db_connection(cursor, cnx)
    

# For 3.2 Widget Three: MySQL Table - Delete Faculty
def delete_faculty(faculty_id: int) -> bool:
    """Soft delete a faculty member by marking it as deleted using a Transaction."""
    cnx, cursor = None, None
    try:
        cnx = get_db_connection()
        cursor = cnx.cursor()

        # Start transaction
        cnx.start_transaction()

        # Mark the faculty record as deleted
        cursor.execute("UPDATE faculty SET is_deleted = TRUE WHERE id = %s", (faculty_id,))
        
        # Commit transaction to finalize changes
        cnx.commit()
        return True

    except Exception as e:
        print("Error deleting faculty member:", e)

        # Rollback transaction in case of failure
        if cnx:
            cnx.rollback()
        return False

    finally:
        close_db_connection(cursor, cnx)


# For 3.3 Widget Three: MySQL Table - Restore Faculty
def restore_faculty() -> bool:
    """Restore all faculty members by setting is_deleted back to FALSE using a Transaction."""
    cnx, cursor = None, None
    try:
        cnx = get_db_connection()
        cursor = cnx.cursor()

        # Start transaction
        cnx.start_transaction()

        # Restore all soft-deleted faculty members
        cursor.execute("UPDATE faculty SET is_deleted = FALSE WHERE is_deleted = TRUE")

        # Commit transaction to finalize changes
        cnx.commit()
        return True

    except Exception as e:
        print("Error restoring faculty members:", e)

        # Rollback transaction in case of failure
        if cnx:
            cnx.rollback()
        return False

    finally:
        close_db_connection(cursor, cnx)


# For 4.1 Widget Four: MongoDB Bar Chart (with MySQL option) - Database Dropdown
def get_all_universities() -> List[str]:
    """Fetch all universities from the MySQL database."""
    cnx, cursor = None, None
    try:
        cnx = get_db_connection()
        cursor = cnx.cursor()
        cursor.execute("SELECT DISTINCT(name) FROM university")
        results = cursor.fetchall()
        return [str(university[0]) for university in results]  # (university,) -> university
    except Exception as e:
        print("Error fetching universities:", e)
        return []
    finally:
        close_db_connection(cursor, cnx)


# For 4.2 Widget Four: MongoDB Bar Chart (with MySQL option)
def find_top_faculties_with_highest_KRC_keyword_sql(keyword: str, university: str) -> List[Tuple[str, float]]:
    """Find top faculties with highest KRC for the selected keyword and affiliation."""
    cnx, cursor = None, None
    try:
        cnx = get_db_connection()
        cursor = cnx.cursor()

        # Query the view to fetch top faculties with highest KRC
        query = """SELECT faculty.name, 
                   ROUND(SUM(publication_keyword.score * publication.num_citations), 2) AS KRC
                   FROM faculty, faculty_publication, publication, publication_keyword, keyword, university
                   WHERE faculty.id = faculty_publication.faculty_Id
                   AND faculty_publication.publication_Id = publication.ID
                   AND publication.ID = publication_keyword.publication_id
                   AND publication_keyword.keyword_id = keyword.id
                   AND keyword.name = %s
                   AND faculty.university_id = university.id
                   AND university.name = %s
                   GROUP BY faculty.id ORDER BY KRC DESC LIMIT 10;
                   """
        cursor.execute(query, (keyword, university))
        results = cursor.fetchall()
        return [(str(row[0]), _safe_float(row[1])) for row in results]  # [(faculty, KRC), ...]
    except Exception as e:
        print(f"Error fetching faculties for keyword '{keyword}' and affiliation '{university}':", e)
        return []
    finally:
        close_db_connection(cursor, cnx)


# For 5.1 Widget Five: MySQL Table (Keeping Neo4j for now, MySQL alternative available)
def faculty_interested_in_keywords_mysql(university_name: str) -> List[Tuple[str, str, int]]:
    """Fetch top keywords that faculty at a selected university are interested in from the MySQL database."""
    cnx, cursor = None, None
    try:
        cnx = get_db_connection()
        cursor = cnx.cursor()
        
        # Query to get top 10 keywords by faculty count for a given university
        # Join: university -> faculty -> faculty_keyword -> keyword
        query = """SELECT keyword.id, keyword.name, COUNT(DISTINCT faculty.id) AS faculty_count
                   FROM university
                   INNER JOIN faculty ON university.id = faculty.university_id
                   INNER JOIN faculty_keyword ON faculty.id = faculty_keyword.faculty_id
                   INNER JOIN keyword ON faculty_keyword.keyword_id = keyword.id
                   WHERE university.name = %s
                   AND faculty.is_deleted = FALSE
                   AND (keyword.is_deleted IS NULL OR keyword.is_deleted = FALSE)
                   GROUP BY keyword.id, keyword.name
                   ORDER BY faculty_count DESC
                   LIMIT 10"""
        cursor.execute(query, (university_name,))
        results = cursor.fetchall()
        return [(str(row[0]), str(row[1]), _safe_int(row[2])) for row in results]  # [(id, keyword, count), ...]
    except Exception as e:
        print(f"Error fetching faculty keyword data for '{university_name}':", e)
        import traceback
        traceback.print_exc()
        return []
    finally:
        close_db_connection(cursor, cnx)


# For 5.2 Widget Five: MySQL Table - Count Keywords (Keeping Neo4j for now, MySQL alternative available)
def get_keyword_count_mysql() -> int:
    """Get the total number of keywords in the MySQL database (excluding deleted ones)."""
    cnx, cursor = None, None
    try:
        cnx = get_db_connection()
        cursor = cnx.cursor()
        
        # Check if 'is_deleted' column exists in keyword table
        cursor.execute("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = DATABASE() 
            AND TABLE_NAME = 'keyword' 
            AND COLUMN_NAME = 'is_deleted'
        """)
        result = cursor.fetchone()
        column_exists = (_safe_int(result[0]) > 0) if result else False
        
        if not column_exists:
            try:
                cursor.execute("ALTER TABLE keyword ADD COLUMN is_deleted BOOLEAN DEFAULT FALSE")
                cnx.commit()
            except Exception as alter_error:
                print(f"Error adding is_deleted column to keyword table: {alter_error}")
                # Continue even if column creation fails
        
        # Count active (non-deleted) keywords
        cursor.execute("SELECT COUNT(*) FROM keyword WHERE is_deleted IS NULL OR is_deleted = FALSE")
        result = cursor.fetchone()
        return _safe_int(result[0]) if result else 0
    except Exception as e:
        print(f"Error fetching keyword count: {e}")
        return 0
    finally:
        close_db_connection(cursor, cnx)


# For 5.3 Widget Five: MySQL Table - Delete Keywords (Keeping Neo4j for now, MySQL alternative available)
def delete_keyword_mysql(keyword_id: str) -> bool:
    """Soft delete a keyword from the MySQL database using a Transaction."""
    cnx, cursor = None, None
    try:
        cnx = get_db_connection()
        cursor = cnx.cursor()
        
        # First, ensure is_deleted column exists (do this separately to avoid transaction issues)
        cursor.execute("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = DATABASE() 
            AND TABLE_NAME = 'keyword' 
            AND COLUMN_NAME = 'is_deleted'
        """)
        result = cursor.fetchone()
        column_exists = (_safe_int(result[0]) > 0) if result else False
        
        if not column_exists:
            try:
                cursor.execute("ALTER TABLE keyword ADD COLUMN is_deleted BOOLEAN DEFAULT FALSE")
                cnx.commit()
            except Exception as alter_error:
                print(f"Error adding is_deleted column to keyword table: {alter_error}")
                # Continue even if column creation fails
                # If there was an error, ensure we're in a clean state
                try:
                    cnx.rollback()
                except:
                    pass
        
        # Validate keyword_id
        if not keyword_id or keyword_id.strip() == "":
            print(f"Invalid keyword_id: empty or None")
            return False
        
        # Now handle the delete operation
        # Try converting to int first (most MySQL IDs are integers)
        keyword_id_int = None
        try:
            keyword_id_int = int(keyword_id)
        except (ValueError, TypeError):
            pass  # Keep as string if conversion fails
        
        # Ensure we're not in a transaction before starting a new one
        try:
            # Try to start transaction, but if one is already in progress, rollback first
            cnx.start_transaction()
        except Exception as tx_error:
            # If transaction already in progress, rollback and try again
            if "already in progress" in str(tx_error).lower():
                cnx.rollback()
                cnx.start_transaction()
            else:
                raise
        
        # Soft delete the keyword - try int first, then string
        rows_affected = 0
        if keyword_id_int is not None:
            cursor.execute("UPDATE keyword SET is_deleted = TRUE WHERE id = %s", (keyword_id_int,))
            rows_affected = cursor.rowcount
            if rows_affected == 0:
                # If int didn't work, try as string
                cnx.rollback()
                # Start a new transaction for the string attempt
                try:
                    cnx.start_transaction()
                except Exception as tx_error:
                    if "already in progress" in str(tx_error).lower():
                        cnx.rollback()
                        cnx.start_transaction()
                    else:
                        raise
                cursor.execute("UPDATE keyword SET is_deleted = TRUE WHERE id = %s", (keyword_id,))
                rows_affected = cursor.rowcount
        else:
            cursor.execute("UPDATE keyword SET is_deleted = TRUE WHERE id = %s", (keyword_id,))
            rows_affected = cursor.rowcount
        
        if rows_affected > 0:
            cnx.commit()
            print(f"Successfully deleted keyword with id: {keyword_id}")
            return True
        else:
            cnx.rollback()
            print(f"No keyword found with id: {keyword_id} (tried as {'int and string' if keyword_id_int is not None else 'string'})")
            return False
        
    except Exception as e:
        print(f"Error deleting keyword '{keyword_id}':", e)
        import traceback
        traceback.print_exc()
        if cnx:
            cnx.rollback()
        return False
    finally:
        close_db_connection(cursor, cnx)


# For 5.4 Widget Five: MySQL Table - Restore Keywords (Keeping Neo4j for now, MySQL alternative available)
def restore_keyword_mysql() -> bool:
    """Restore all deleted keywords in the MySQL database using a Transaction."""
    cnx, cursor = None, None
    try:
        cnx = get_db_connection()
        cursor = cnx.cursor()
        
        # Check if 'is_deleted' column exists
        cursor.execute("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = DATABASE() 
            AND TABLE_NAME = 'keyword' 
            AND COLUMN_NAME = 'is_deleted'
        """)
        result = cursor.fetchone()
        column_exists = (_safe_int(result[0]) > 0) if result else False
        
        if not column_exists:
            # If column doesn't exist, ensure it exists first
            try:
                cursor.execute("ALTER TABLE keyword ADD COLUMN is_deleted BOOLEAN DEFAULT FALSE")
                cnx.commit()
                print("Added is_deleted column to keyword table")
            except Exception as alter_error:
                print(f"Error adding is_deleted column to keyword table: {alter_error}")
                # If there was an error, ensure we're in a clean state
                try:
                    cnx.rollback()
                except:
                    pass
            # No keywords to restore if column didn't exist
            return True
        
        # Start transaction - handle case where transaction is already in progress
        try:
            cnx.start_transaction()
        except Exception as tx_error:
            # If transaction already in progress, rollback and try again
            if "already in progress" in str(tx_error).lower():
                cnx.rollback()
                cnx.start_transaction()
            else:
                raise
        
        # Restore all soft-deleted keywords by setting is_deleted = FALSE
        cursor.execute("UPDATE keyword SET is_deleted = FALSE WHERE is_deleted = TRUE")
        
        rows_affected = cursor.rowcount
        print(f"Restored {rows_affected} keyword(s)")
        
        # Commit transaction to finalize changes
        cnx.commit()
        return True
        
    except Exception as e:
        print(f"Error restoring keywords: {e}")
        import traceback
        traceback.print_exc()
        
        # Rollback transaction in case of failure
        if cnx:
            cnx.rollback()
        return False
    finally:
        close_db_connection(cursor, cnx)


# For 6.2 Widget Six: Neo4j Sunburst Chart - University Information
def get_university_information(university_name: str) -> List[Tuple[str, int, str]]:
    """Fetch university information based on the university name."""
    cnx, cursor = None, None
    try:
        cnx = get_db_connection()
        cursor = cnx.cursor()
        
        query = """SELECT university.name, COUNT(faculty.id) AS faculty_count, university.photo_url
                   FROM university, faculty
                   WHERE university.name = %s
                   AND university.id = faculty.university_id
                   GROUP BY university.name, university.photo_url;"""
        cursor.execute(query, (university_name,))
        results = cursor.fetchall()
        return [(str(row[0]), _safe_int(row[1]), str(row[2]) if row[2] else "") for row in results]  # [(name, faculty_count, photo_url), ...]
    except Exception as e:
        print("Error fetching university information:", e)
        return []
    finally:
        close_db_connection(cursor, cnx)
