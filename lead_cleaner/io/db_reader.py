"""
SQLite Database Reader Module

Provides functionality to load data from SQLite .db/.sqlite files.
Uses Python's built-in sqlite3 library (zero external dependencies).
"""

import sqlite3
import pandas as pd
import os
from typing import List, Optional

from lead_cleaner.logging.logger import PipelineLogger
from lead_cleaner.exceptions import ValidationError


def list_tables(conn: sqlite3.Connection) -> List[str]:
    """
    List all user tables in the SQLite database.
    
    Args:
        conn: Active SQLite connection
        
    Returns:
        List of table names
    """
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
    tables = [row[0] for row in cursor.fetchall()]
    return tables


def select_table_interactive(tables: List[str], file_path: str) -> str:
    """
    Interactively prompt user to select a table from the database.
    
    Args:
        tables: List of available table names
        file_path: Path to the database file (for display)
        
    Returns:
        Name of the selected table
    """
    print(f"\n📂 Found {len(tables)} tables in {os.path.basename(file_path)}:")
    for i, table in enumerate(tables):
        print(f"   [{i}] {table}")
    
    while True:
        try:
            choice = input("\n👉 Enter the index of the table to clean: ").strip()
            index = int(choice)
            if 0 <= index < len(tables):
                return tables[index]
            else:
                print(f"   ⚠️  Please enter a number between 0 and {len(tables) - 1}")
        except ValueError:
            # Check if user typed the table name directly
            if choice in tables:
                return choice
            print(f"   ⚠️  Invalid input. Enter a number (0-{len(tables) - 1}) or table name.")


def load_from_sqlite(file_path: str, logger: PipelineLogger) -> pd.DataFrame:
    """
    Load data from a SQLite database file into a Pandas DataFrame.
    
    If the database contains multiple tables, prompts the user to select one.
    If only one table exists, it is automatically selected.
    
    Args:
        file_path: Path to the SQLite .db/.sqlite file
        logger: PipelineLogger instance for logging events
        
    Returns:
        DataFrame containing the selected table's data
        
    Raises:
        ValidationError: If database is unreadable, empty, or contains no tables
    """
    if not os.path.exists(file_path):
        error_msg = f"Database file not found: {file_path}"
        logger.log_event(phase="SETUP", action="VALIDATION_FAILED", reason=error_msg)
        raise ValidationError(error_msg)
    
    conn = None
    try:
        # Connect to the database (read-only would be ideal but sqlite3 doesn't support it directly)
        conn = sqlite3.connect(file_path)
        
        # List available tables
        tables = list_tables(conn)
        
        if not tables:
            error_msg = "No tables found in database."
            logger.log_event(phase="SETUP", action="VALIDATION_FAILED", reason=error_msg)
            raise ValidationError(error_msg)
        
        # Select table (auto-select if only one)
        if len(tables) == 1:
            selected_table = tables[0]
            print(f"📋 Auto-selected table: '{selected_table}'")
        else:
            selected_table = select_table_interactive(tables, file_path)
        
        logger.log_event(
            phase="SETUP",
            action="TABLE_SELECTED",
            reason=f"Selected table '{selected_table}' from {os.path.basename(file_path)}"
        )
        
        print(f"⏳ Loading table '{selected_table}'...")
        
        # Load table into DataFrame
        # IMPORTANT: Use parameterized queries in production, but table names can't be parameterized
        # We trust the table name as it comes from sqlite_master
        df = pd.read_sql_query(f'SELECT * FROM "{selected_table}"', conn)
        
        if df.empty:
            logger.log_event(
                phase="SETUP",
                action="VALIDATION_WARNING",
                reason=f"Table '{selected_table}' is empty."
            )
            print(f"⚠️  Warning: Table '{selected_table}' is empty.")
        
        logger.log_event(
            phase="SETUP",
            action="DATABASE_LOADED",
            reason=f"Loaded {len(df)} rows from table '{selected_table}'"
        )
        
        print(f"✅ Loaded {len(df)} rows from '{selected_table}'")
        
        return df
        
    except sqlite3.DatabaseError as e:
        error_msg = f"Error: Unable to read database structure. Is this a valid SQLite file? ({str(e)})"
        logger.log_event(phase="SETUP", action="DATABASE_ERROR", reason=str(e))
        raise ValidationError(error_msg)
        
    except Exception as e:
        error_msg = f"Unexpected error reading database: {str(e)}"
        logger.log_event(phase="SETUP", action="DATABASE_ERROR", reason=str(e))
        raise ValidationError(error_msg)
        
    finally:
        # CRITICAL: Always close connection to prevent file locks
        if conn:
            conn.close()
