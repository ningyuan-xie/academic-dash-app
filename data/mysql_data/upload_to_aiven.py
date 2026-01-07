#!/usr/bin/env python3
"""
Script to upload a .sql file to Aiven MySQL database in chunks.
This handles large SQL files by splitting them into manageable pieces.
Usage: python upload_to_aiven.py [path/to/your/file.sql]
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

def clean_aiven_database():
    """Completely clean the Aiven database by dropping all user databases."""
    connection = None
    cursor = None
    try:
        # Connect to Aiven MySQL without specifying a database
        print("🔗 Connecting to Aiven MySQL for cleanup...")
        connection = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=int(os.getenv("DB_PORT", 3306)),
            ssl_disabled=False,
            autocommit=False,
            connect_timeout=30,
            use_unicode=True,
            charset='utf8mb4'
        )

        cursor = connection.cursor()

        print("🧹 Starting complete database cleanup...")

        # Get all databases
        cursor.execute("SHOW DATABASES")
        databases = []
        for row in cursor.fetchall():
            if isinstance(row, (list, tuple)) and len(row) > 0:
                db_name = row[0]
                if db_name and str(db_name).strip():
                    databases.append(str(db_name).strip())

        # Filter out system databases
        system_databases = {
            'information_schema', 'mysql', 'performance_schema', 
            'sys', 'mysql_innodb_cluster_metadata'
        }
        user_databases = [db for db in databases if db not in system_databases]

        if not user_databases:
            print("   ℹ️  No user databases found to clean")
            return True

        print(f"   📋 Found {len(user_databases)} user databases to drop:")
        for db in user_databases:
            print(f"      - {db}")

        # Drop each user database
        for db in user_databases:
            try:
                print(f"   🗑️  Dropping database: {db}")
                cursor.execute(f"DROP DATABASE IF EXISTS `{db}`")
                print(f"      ✅ Dropped {db}")
            except Error as e:
                print(f"      ⚠️  Failed to drop {db}: {e}")

        # Commit the cleanup
        connection.commit()
        print("   💾 Cleanup committed")
        print("   ✅ Database cleanup completed successfully")

        return True

    except Error as e:
        print(f"   ❌ Database cleanup failed: {e}")
        error_msg = str(e).lower()
        if "read-only" in error_msg:
            print(f"   🔍 Your Aiven MySQL service might be in read-only mode.")
            print(f"   Please check your Aiven console to ensure the service is fully running.")
        return False

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

def split_sql_file(sql_file_path, chunk_size_mb=5):
    """Split a large SQL file into smaller chunks."""
    chunk_size_bytes = chunk_size_mb * 1024 * 1024

    # Create temporary directory for chunks
    temp_dir = tempfile.mkdtemp(prefix="sql_chunks_")
    chunk_files = []

    with open(sql_file_path, 'r', encoding='utf-8') as file:
        chunk_num = 1
        current_chunk_size = 0
        current_chunk_content = []

        for line in file:
            current_chunk_content.append(line)
            current_chunk_size += len(line.encode('utf-8'))

            # If we've reached the chunk size and we're at a statement boundary
            if current_chunk_size >= chunk_size_bytes and line.strip().endswith(';'):
                # Write this chunk
                chunk_filename = os.path.join(temp_dir, f"chunk_{chunk_num:03d}.sql")
                with open(chunk_filename, 'w', encoding='utf-8') as chunk_file:
                    chunk_file.writelines(current_chunk_content)

                chunk_files.append(chunk_filename)
                print(f"   📄 Created chunk {chunk_num}: {current_chunk_size / 1024 / 1024:.1f}MB")

                # Reset for next chunk
                chunk_num += 1
                current_chunk_size = 0
                current_chunk_content = []

        # Write the final chunk if there's remaining content
        if current_chunk_content:
            chunk_filename = os.path.join(temp_dir, f"chunk_{chunk_num:03d}.sql")
            with open(chunk_filename, 'w', encoding='utf-8') as chunk_file:
                chunk_file.writelines(current_chunk_content)
            chunk_files.append(chunk_filename)
            print(f"   📄 Created chunk {chunk_num}: {current_chunk_size / 1024 / 1024:.1f}MB")

    return temp_dir, chunk_files

def upload_chunk(chunk_file_path, chunk_num, total_chunks):
    """Upload a single SQL chunk to the database."""
    connection = None
    cursor = None
    try:
        # Connect to Aiven MySQL without specifying database initially
        print(f"🔗 Connecting to Aiven MySQL for chunk {chunk_num}...")
        connection = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=int(os.getenv("DB_PORT", 3306)),
            ssl_disabled=False,
            autocommit=False,
            connect_timeout=30,
            use_unicode=True,
            charset='utf8mb4'
        )

        cursor = connection.cursor()

        print(f"⬆️  Uploading chunk {chunk_num}/{total_chunks}: {os.path.basename(chunk_file_path)}")

        # Configure MySQL for this chunk
        cursor.execute("SET sql_require_primary_key = 0")
        cursor.execute("SET autocommit = 0")
        cursor.execute("SET unique_checks = 0") 
        cursor.execute("SET foreign_key_checks = 0")
        cursor.execute("SET sql_mode = 'NO_AUTO_VALUE_ON_ZERO'")

        # Only drop and recreate database for the first chunk
        if chunk_num == 1:
            print(f"      🗑️  Dropping existing academicworldmini database...")
            cursor.execute("DROP DATABASE IF EXISTS academicworldmini")
            print(f"      🆕 Creating fresh academicworldmini database...")
            cursor.execute("CREATE DATABASE academicworldmini")

        # Use the academicworldmini database
        cursor.execute("USE academicworldmini")
        print(f"      📂 Using database: academicworldmini")

        # Read and execute the chunk
        with open(chunk_file_path, 'r', encoding='utf-8') as file:
            sql_content = file.read()

        # Split into statements
        statements = []
        current_statement = ""
        in_string = False
        escape_next = False

        for char in sql_content:
            if escape_next:
                current_statement += char
                escape_next = False
                continue

            if char == '\\':
                escape_next = True
                current_statement += char
                continue

            if char == "'" and not escape_next:
                in_string = not in_string

            if char == ';' and not in_string:
                statement = current_statement.strip()
                if statement and not statement.startswith('--') and not statement.startswith('/*'):
                    statements.append(statement)
                current_statement = ""
            else:
                current_statement += char

        # Execute statements
        success_count = 0
        error_count = 0

        for i, statement in enumerate(statements):
            try:
                cursor.execute(statement)
                success_count += 1

                if (i + 1) % 100 == 0:
                    print(f"      Executed {i + 1}/{len(statements)} statements...")

            except Error as e:
                error_count += 1
                if error_count <= 5:  # Limit error output
                    error_msg = str(e)[:100]
                    print(f"      ⚠️  Error in statement {i + 1}: {error_msg}...")

                    # Check for critical errors that should stop the process
                    if "table" in error_msg.lower() and "full" in error_msg.lower():
                        print(f"      🛑 Critical: Database storage limit reached!")
                        raise e

        # Commit the chunk
        try:
            connection.commit()
            print(f"   💾 Committed chunk {chunk_num}")
        except Error as commit_error:
            print(f"   ⚠️  Commit failed for chunk {chunk_num}: {commit_error}")
            # Try to rollback
            try:
                connection.rollback()
                print(f"   🔄 Rolled back chunk {chunk_num}")
            except:
                pass

        # Re-enable constraints for this connection
        try:
            cursor.execute("SET foreign_key_checks = 1")
            cursor.execute("SET unique_checks = 1")
            cursor.execute("SET autocommit = 1")
        except Error as cleanup_error:
            print(f"   ⚠️  Cleanup failed: {cleanup_error}")

        print(f"   ✅ Chunk {chunk_num} completed: {success_count} statements executed, {error_count} errors")

        return True, success_count, error_count

    except Error as e:
        print(f"   ❌ Chunk {chunk_num} failed: {e}")
        error_msg = str(e).lower()
        if "read-only" in error_msg:
            print(f"   🔍 Your Aiven MySQL service might be in read-only mode.")
            print(f"   Please check your Aiven console to ensure the service is fully running.")
        elif "table" in error_msg and "full" in error_msg:
            print(f"   💾 Database storage limit reached! Your Aiven plan may have size restrictions.")
            print(f"   Consider upgrading your Aiven plan or reducing your dataset size.")
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
    """Upload a SQL file in chunks to handle large files."""

    if not os.path.exists(sql_file_path):
        print(f"❌ Error: File '{sql_file_path}' not found.")
        return False

    # Get file size
    file_size_mb = os.path.getsize(sql_file_path) / 1024 / 1024
    print(f"📊 SQL file size: {file_size_mb:.1f}MB")

    if file_size_mb <= 5:
        # Small file, upload directly
        print("📤 File is small enough, uploading directly...")
        return upload_chunk(sql_file_path, 1, 1)[0]

    # Large file, split into chunks
    print(f"📁 File is large ({file_size_mb:.1f}MB), splitting into chunks...")

    temp_dir = None
    try:
        temp_dir, chunk_files = split_sql_file(sql_file_path)
        print(f"✅ Created {len(chunk_files)} chunks")

        total_success = 0
        total_errors = 0

        # Upload chunks sequentially
        for i, chunk_file in enumerate(chunk_files, 1):
            success, success_count, error_count = upload_chunk(chunk_file, i, len(chunk_files))

            if not success:
                print(f"❌ Upload failed at chunk {i}")
                return False

            total_success += success_count
            total_errors += error_count

            # Longer delay between chunks for smaller instance
            if i < len(chunk_files):
                print(f"   ⏳ Waiting 5 seconds before next chunk...")
                time.sleep(5)

        print(f"\n📈 Upload Summary:")
        print(f"   ✅ Total statements executed: {total_success}")
        print(f"   ⚠️  Total errors: {total_errors}")
        print(f"   📦 Chunks uploaded: {len(chunk_files)}")

        return True

    finally:
        # Clean up temporary files
        if temp_dir and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
            print("🧹 Cleaned up temporary files")

def main():
    # Default to the academicworldmini.sql file if no argument provided
    default_sql_path = "data/mysql_data/academicworldmini.sql"

    if len(sys.argv) == 1:
        # No argument provided, use default
        sql_file_path = default_sql_path
        print(f"ℹ️  No file specified, using default: {sql_file_path}")
    elif len(sys.argv) == 2:
        # File path provided as argument
        sql_file_path = sys.argv[1]
    else:
        print("Usage: python upload_to_aiven.py [path/to/your/file.sql]")
        print(f"If no path is provided, will use default: {default_sql_path}")
        sys.exit(1)

    print("🚀 Starting chunked SQL file upload to Aiven MySQL...")
    print(f"   Host: {os.getenv('DB_HOST')}")
    print(f"   Database: {os.getenv('DB_NAME')}")
    print(f"   File: {sql_file_path}")
    print()

    # Clean up existing databases before uploading
    print("🧹 Starting database cleanup...")
    clean_aiven_database()
    print("✅ Database cleanup completed.")

    success = upload_sql_file_chunked(sql_file_path)

    if success:
        print("\n🎉 Upload completed successfully!")
        print("   Your database is now ready on Aiven.")
        print("   You can update your application to use the new connection details.")
    else:
        print("\n❌ Upload failed. Please check the errors above.")
        print("Common issues:")
        print("  1. Check your .env file has correct Aiven credentials")
        print("  2. Ensure your Aiven MySQL service is running") 
        print("  3. Verify network connectivity to Aiven")

if __name__ == "__main__":
    main() 