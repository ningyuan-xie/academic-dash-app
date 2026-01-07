#!/usr/bin/env python3
"""
Script to upload a .sql file to Railway MySQL database in chunks.
This handles large SQL files by splitting them into manageable pieces.
Usage: python upload_to_railway.py [path/to/your/file.sql]
"""

import sys
import os
import time
import tempfile
import shutil
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

# Load environment variables (override any existing ones)
load_dotenv(override=True)

def print_progress_bar(current, total, start_time, prefix="Progress", length=50):
    """Print a visual progress bar with percentage and ETA."""
    percentage = current / total
    filled_length = int(length * percentage)
    bar = '█' * filled_length + '░' * (length - filled_length)

    # Calculate elapsed time and ETA
    elapsed_time = time.time() - start_time
    if current > 0:
        eta_seconds = (elapsed_time / current) * (total - current)
        eta_str = f"ETA: {int(eta_seconds // 60)}m {int(eta_seconds % 60)}s"
    else:
        eta_str = "ETA: --:--"

    print(f'\r{prefix}: |{bar}| {current}/{total} ({percentage:.1%}) {eta_str}', end='', flush=True)

    if current == total:
        print()  # New line when complete

def clear_railway_database():
    """Clear the Railway database with progress indicators before upload."""
    print('🗑️  Starting Railway database cleanup...')

    try:
        print('   📡 [1/4] Connecting to Railway...', end='', flush=True)
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'), 
            password=os.getenv('DB_PASSWORD'),
            port=int(os.getenv('DB_PORT', 3306))
        )
        print(' ✅')

        cursor = connection.cursor()
        db_name = os.getenv('DB_NAME', 'academicworld')

        print(f'   🗃️  [2/4] Dropping existing database "{db_name}"...', end='', flush=True)
        cursor.execute(f'DROP DATABASE IF EXISTS {db_name}')
        time.sleep(0.5)  # Small delay for visual feedback
        print(' ✅')

        print(f'   ✨ [3/4] Creating fresh database "{db_name}"...', end='', flush=True)
        cursor.execute(f'CREATE DATABASE {db_name}')
        time.sleep(0.5)
        print(' ✅')

        print('   💾 [4/4] Committing changes...', end='', flush=True)
        connection.commit()
        print(' ✅')

        print('')
        print('🎉 Railway database cleared successfully!')
        print(f'   Database "{db_name}" is now empty and ready for new data')

        return True

    except Exception as e:
        print(f' ❌')
        print(f'❌ Error clearing database: {e}')
        return False
    finally:
        if 'connection' in locals():
            connection.close()
            print('   🔌 Connection closed')

def split_sql_file(sql_file_path, chunk_size_mb=15):
    """Split a large SQL file into larger chunks to reduce connection overhead.
    Ensures all CREATE TABLE statements go in the first chunk."""
    chunk_size_bytes = chunk_size_mb * 1024 * 1024

    # Create temporary directory for chunks
    temp_dir = tempfile.mkdtemp(prefix="railway_sql_chunks_")
    chunk_files = []

    # Read and parse the entire file first
    with open(sql_file_path, 'r', encoding='utf-8') as file:
        content = file.read()

    # Split into statements
    statements = []
    current_statement = ""
    in_string = False
    escape_next = False

    for char in content:
        if escape_next:
            current_statement += char
            escape_next = False
            continue

        if char == '\\':
            escape_next = True
            current_statement += char
            continue

        if char in ('"', "'") and not escape_next:
            in_string = not in_string

        if char == ';' and not in_string:
            statement = current_statement.strip()
            if statement and not statement.startswith('--'):
                statements.append(statement + ';')
            current_statement = ""
        else:
            current_statement += char

    # Separate CREATE statements from INSERT statements
    create_statements = []
    other_statements = []

    for stmt in statements:
        if stmt.upper().strip().startswith('CREATE') or stmt.upper().strip().startswith('USE'):
            create_statements.append(stmt)
        else:
            other_statements.append(stmt)

    # Chunk 1: All CREATE statements and initial setup
    chunk_num = 1
    chunk_1_content = '\n'.join(create_statements) + '\n'
    chunk_filename = os.path.join(temp_dir, f"railway_chunk_{chunk_num:03d}.sql")

    with open(chunk_filename, 'w', encoding='utf-8') as chunk_file:
        chunk_file.write(chunk_1_content)

    chunk_files.append(chunk_filename)
    chunk_1_size = len(chunk_1_content.encode('utf-8')) / 1024 / 1024
    print(f"   📄 Created chunk {chunk_num}: {chunk_1_size:.1f}MB (CREATE tables)")

    # Remaining chunks: INSERT statements only
    chunk_num = 2
    current_chunk_content = []
    current_chunk_size = 0

    for stmt in other_statements:
        stmt_with_newline = stmt + '\n'
        stmt_size = len(stmt_with_newline.encode('utf-8'))

        # If adding this statement would exceed chunk size, write current chunk
        if current_chunk_size > 0 and current_chunk_size + stmt_size > chunk_size_bytes:
            chunk_filename = os.path.join(temp_dir, f"railway_chunk_{chunk_num:03d}.sql")
            with open(chunk_filename, 'w', encoding='utf-8') as chunk_file:
                chunk_file.write('\n'.join(current_chunk_content) + '\n')

            chunk_files.append(chunk_filename)
            print(f"   📄 Created chunk {chunk_num}: {current_chunk_size / 1024 / 1024:.1f}MB")

            # Reset for next chunk
            chunk_num += 1
            current_chunk_content = []
            current_chunk_size = 0

        current_chunk_content.append(stmt)
        current_chunk_size += stmt_size

    # Write the final chunk if there's remaining content
    if current_chunk_content:
        chunk_filename = os.path.join(temp_dir, f"railway_chunk_{chunk_num:03d}.sql")
        with open(chunk_filename, 'w', encoding='utf-8') as chunk_file:
            chunk_file.write('\n'.join(current_chunk_content) + '\n')

        chunk_files.append(chunk_filename)
        print(f"   📄 Created chunk {chunk_num}: {current_chunk_size / 1024 / 1024:.1f}MB")

    return temp_dir, chunk_files

def get_railway_connection():
    """Create Railway MySQL connection with optimized settings."""
    # Railway supports both individual env vars and DATABASE_URL
    database_url = os.getenv("DATABASE_URL")

    if database_url:
        # Parse DATABASE_URL format: mysql://user:password@host:port/database
        print("🔗 Using DATABASE_URL for Railway connection...")
        import urllib.parse
        parsed = urllib.parse.urlparse(database_url)

        connection_config = {
            'host': parsed.hostname,
            'user': parsed.username,
            'password': parsed.password,
            'database': parsed.path[1:],  # Remove leading '/'
            'port': parsed.port or 3306,
        }
    else:
        # Use individual environment variables
        print("🔗 Using individual env vars for Railway connection...")
        connection_config = {
            'host': os.getenv("MYSQL_HOST") or os.getenv("DB_HOST"),
            'user': os.getenv("MYSQL_USER") or os.getenv("DB_USER"),
            'password': os.getenv("MYSQL_PASSWORD") or os.getenv("DB_PASSWORD"),
            'database': os.getenv("MYSQL_DATABASE") or os.getenv("DB_NAME"),
            'port': int(os.getenv("MYSQL_PORT") or os.getenv("DB_PORT") or 3306),
        }

    # Railway-optimized connection settings
    connection_config.update({
        'ssl_disabled': False,
        'autocommit': False,
        'connect_timeout': 60,  # Railway can be slower to connect
        'use_unicode': True,
        'charset': 'utf8mb4',
        'sql_mode': '',
        'raise_on_warnings': False,
    })

    return mysql.connector.connect(**connection_config)

def upload_chunk(chunk_file_path, chunk_num, total_chunks):
    """Upload a single SQL chunk to Railway MySQL database."""
    connection = None
    cursor = None
    try:
        print(f"🚂 Connecting to Railway MySQL for chunk {chunk_num}...")
        connection = get_railway_connection()

        cursor = connection.cursor(buffered=True)

        print(f"⬆️  Uploading chunk {chunk_num}/{total_chunks}: {os.path.basename(chunk_file_path)}")

        # Configure MySQL for optimal Railway performance
        railway_settings = [
            "SET SESSION sql_require_primary_key = 0",
            "SET SESSION autocommit = 0",
            "SET SESSION unique_checks = 0", 
            "SET SESSION foreign_key_checks = 0",
            "SET SESSION sql_mode = 'NO_AUTO_VALUE_ON_ZERO'",
            "SET SESSION innodb_lock_wait_timeout = 120",
            "SET SESSION max_execution_time = 0",
        ]

        for setting in railway_settings:
            try:
                cursor.execute(setting)
            except Error as e:
                # Some settings might not be available on Railway, continue
                print(f"      ⚠️  Setting ignored: {setting} ({e})")

        # Get database name from environment
        db_name = os.getenv('DB_NAME', 'academicworld')

        # Just use the database (cleanup already done at start)
        cursor.execute(f"USE {db_name}")
        print(f"      📂 Using database: {db_name}")

        # Read and execute the chunk
        with open(chunk_file_path, 'r', encoding='utf-8') as file:
            sql_content = file.read()

        # Parse SQL statements more carefully for Railway
        statements = []
        current_statement = ""
        in_string = False
        escape_next = False
        string_char = None

        for char in sql_content:
            if escape_next:
                current_statement += char
                escape_next = False
                continue

            if char == '\\':
                escape_next = True
                current_statement += char
                continue

            if char in ('"', "'") and not escape_next:
                if not in_string:
                    in_string = True
                    string_char = char
                elif char == string_char:
                    in_string = False
                    string_char = None

            if char == ';' and not in_string:
                statement = current_statement.strip()
                if (statement and 
                    not statement.startswith('--') and 
                    not statement.startswith('/*') and
                    not statement.upper().startswith('DELIMITER')):
                    statements.append(statement)
                current_statement = ""
            else:
                current_statement += char

        # Execute statements with Railway-optimized batching
        success_count = 0
        error_count = 0
        batch_size = 50  # Smaller batches for Railway
        start_time = time.time()

        print(f"      📊 Executing {len(statements)} statements...")

        for i, statement in enumerate(statements):
            try:
                cursor.execute(statement)
                success_count += 1

                # Commit in smaller batches for Railway stability
                if (i + 1) % batch_size == 0:
                    connection.commit()
                    print_progress_bar(i + 1, len(statements), start_time, 
                                     prefix=f"      Chunk {chunk_num}")

                if (i + 1) % 200 == 0:  # Less frequent updates for cleaner output
                    print(f"\n      ✅ {success_count} statements executed, {error_count} errors so far...")

            except Error as e:
                error_count += 1
                if error_count <= 5:  # Fewer error messages for cleaner output
                    error_msg = str(e)[:100]
                    print(f"\n      ⚠️  Error in statement {i + 1}: {error_msg}...")

                    # Check for Railway-specific errors
                    if "connection" in error_msg.lower():
                        print(f"      🔄 Connection issue detected, will retry...")
                        # Railway connections can be unstable, continue
                    elif "timeout" in error_msg.lower():
                        print(f"      ⏰ Timeout detected - Railway may need longer...")

        # Final progress update
        print_progress_bar(len(statements), len(statements), start_time, 
                         prefix=f"      Chunk {chunk_num}")
        print(f"      ✅ Completed: {success_count} executed, {error_count} errors")

        # Final commit for remaining statements
        try:
            connection.commit()
            print(f"   💾 Final commit for chunk {chunk_num}")
        except Error as commit_error:
            print(f"   ⚠️  Final commit failed for chunk {chunk_num}: {commit_error}")
            try:
                connection.rollback()
                print(f"   🔄 Rolled back chunk {chunk_num}")
            except:
                pass

        # Re-enable constraints
        try:
            cursor.execute("SET SESSION foreign_key_checks = 1")
            cursor.execute("SET SESSION unique_checks = 1")
            cursor.execute("SET SESSION autocommit = 1")
        except Error as cleanup_error:
            print(f"   ⚠️  Cleanup failed: {cleanup_error}")

        print(f"   ✅ Chunk {chunk_num} completed: {success_count} statements executed, {error_count} errors")

        return True, success_count, error_count

    except Error as e:
        print(f"   ❌ Chunk {chunk_num} failed: {e}")
        error_msg = str(e).lower()

        if "access denied" in error_msg:
            print(f"   🔑 Access denied - check your Railway database credentials")
            print(f"   Make sure your DATABASE_URL or MySQL env vars are correct")
        elif "connection" in error_msg:
            print(f"   🌐 Connection issue - Railway services may be restarting")
            print(f"   This is common with Railway, consider retrying")
        elif "table" in error_msg and ("full" in error_msg or "space" in error_msg):
            print(f"   💾 Storage limit reached! Check your Railway plan limits")
        elif "timeout" in error_msg:
            print(f"   ⏰ Timeout - Railway may need more time for large operations")

        return False, 0, 0

    finally:
        if cursor:
            try:
                cursor.close()
            except:
                pass
        if connection and connection.is_connected():
            try:
                connection.close()
            except:
                pass

def upload_sql_file_chunked(sql_file_path):
    """Upload a SQL file in chunks to Railway MySQL."""

    if not os.path.exists(sql_file_path):
        print(f"❌ Error: File '{sql_file_path}' not found.")
        return False

    # Get file size
    file_size_mb = os.path.getsize(sql_file_path) / 1024 / 1024
    print(f"📊 SQL file size: {file_size_mb:.1f}MB")

    if file_size_mb <= 60:
        # Small-medium file, upload directly
        print("📤 File size OK, uploading directly without chunking...")
        return upload_chunk(sql_file_path, 1, 1)[0]

    # Very large file, split into chunks (larger for Railway stability)
    print(f"📁 File is very large ({file_size_mb:.1f}MB), splitting into 15MB chunks for Railway...")

    temp_dir = None
    try:
        temp_dir, chunk_files = split_sql_file(sql_file_path, chunk_size_mb=15)
        print(f"✅ Created {len(chunk_files)} chunks")

        total_success = 0
        total_errors = 0
        failed_chunks = []

        # Upload chunks sequentially with Railway-specific delays
        for i, chunk_file in enumerate(chunk_files, 1):
            print(f"\n🚂 Processing chunk {i}/{len(chunk_files)}...")

            # Railway may need more time between operations
            if i > 1:
                print(f"   ⏳ Waiting 3 seconds for Railway stability...")
                time.sleep(3)

            success, success_count, error_count = upload_chunk(chunk_file, i, len(chunk_files))

            if not success:
                print(f"❌ Chunk {i} failed, adding to retry list")
                failed_chunks.append((i, chunk_file))
            else:
                total_success += success_count
                total_errors += error_count

        # Retry failed chunks once (Railway can be flaky)
        if failed_chunks:
            print(f"\n🔄 Retrying {len(failed_chunks)} failed chunks...")
            for chunk_num, chunk_file in failed_chunks:
                print(f"   🔄 Retrying chunk {chunk_num}...")
                time.sleep(5)  # Longer wait for retries

                success, success_count, error_count = upload_chunk(chunk_file, chunk_num, len(chunk_files))
                if success:
                    total_success += success_count
                    total_errors += error_count
                    print(f"   ✅ Chunk {chunk_num} succeeded on retry!")
                else:
                    print(f"   ❌ Chunk {chunk_num} failed again")

        print(f"\n📈 Railway Upload Summary:")
        print(f"   ✅ Total statements executed: {total_success}")
        print(f"   ⚠️  Total errors: {total_errors}")
        print(f"   📦 Chunks uploaded: {len(chunk_files)}")
        print(f"   🔄 Failed chunks: {len([c for c in failed_chunks if c not in []])}")

        return len(failed_chunks) == 0  # Success if no failed chunks remain

    finally:
        # Clean up temporary files
        if temp_dir and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
            print("🧹 Cleaned up temporary files")

def check_railway_connection():
    """Test Railway connection before upload."""
    try:
        print("🔍 Testing Railway connection...")
        connection = get_railway_connection()
        cursor = connection.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        cursor.close()
        connection.close()

        if result:
            print("✅ Railway connection successful!")
            return True
    except Exception as e:
        print(f"❌ Railway connection failed: {e}")
        print("\n🔧 Troubleshooting tips:")
        print("   1. Check your DATABASE_URL in .env file")
        print("   2. Verify Railway service is running in your dashboard")
        print("   3. Ensure your Railway MySQL service is deployed")
        print("   4. Check if your IP is whitelisted (if required)")
        return False

def main():
    # Default to the academicworld_complete.sql file if no argument provided
    default_sql_path = "data/mysql_data/academicworld_complete.sql"

    if len(sys.argv) == 1:
        # No argument provided, use default
        sql_file_path = default_sql_path
        print(f"ℹ️  No file specified, using default: {sql_file_path}")
    elif len(sys.argv) == 2:
        # File path provided as argument
        sql_file_path = sys.argv[1]
    else:
        print("Usage: python upload_to_railway.py [path/to/your/file.sql]")
        print(f"If no path is provided, will use default: {default_sql_path}")
        sys.exit(1)

    print("🚂 Starting chunked SQL file upload to Railway MySQL...")

    # Show connection info (safely)
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        # Parse and show safe parts of DATABASE_URL
        import urllib.parse
        parsed = urllib.parse.urlparse(database_url)
        print(f"   Host: {parsed.hostname}")
        print(f"   Database: {parsed.path[1:] if parsed.path else 'N/A'}")
    else:
        print(f"   Host: {os.getenv('MYSQL_HOST') or os.getenv('DB_HOST')}")
        print(f"   Database: {os.getenv('MYSQL_DATABASE') or os.getenv('DB_NAME')}")

    print(f"   File: {sql_file_path}")
    print()

    # Test connection first
    if not check_railway_connection():
        print("\n❌ Connection test failed. Please fix connection issues before proceeding.")
        sys.exit(1)

    # Clear existing database before upload
    print()
    if not clear_railway_database():
        print("\n❌ Database cleanup failed. Please fix the issues before proceeding.")
        sys.exit(1)

    print()
    success = upload_sql_file_chunked(sql_file_path)

    if success:
        print("\n🎉 Railway upload completed successfully!")
        print("   Your database is now ready on Railway.")
        print("   Update your .env file to use Railway connection details.")
        print("\n📝 Don't forget to:")
        print("   1. Update your app's DATABASE_URL or MySQL env vars")
        print("   2. Test your application with the new Railway database")
        print("   3. Railway services auto-sleep, so first connection may be slow")
    else:
        print("\n❌ Upload failed or incomplete. Please check the errors above.")
        print("\n🔧 Common Railway issues:")
        print("  1. Services auto-sleep - try running the script again")
        print("  2. Connection timeouts - Railway can be slower than other providers")
        print("  3. Check Railway dashboard for service status")
        print("  4. Verify your DATABASE_URL format: mysql://user:pass@host:port/db")

if __name__ == "__main__":
    main() 