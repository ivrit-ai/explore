#!/usr/bin/env python3
"""
Script to query the database used in the explore app.
This demonstrates how to connect to the database and search for example words.
"""

import sqlite3
import os
from pathlib import Path

def connect_to_db():
    """Connect to the SQLite database used by the app."""
    db_path = "explore.sqlite"
    
    if not os.path.exists(db_path):
        print(f"Database file '{db_path}' not found!")
        print("Make sure you're running this script from the project root directory.")
        return None
    
    try:
        conn = sqlite3.connect(db_path)
        print(f"Successfully connected to database: {db_path}")
        return conn
    except sqlite3.Error as e:
        print(f"Error connecting to database: {e}")
        return None

def show_database_info(conn):
    """Show basic information about the database."""
    cursor = conn.cursor()
    
    # Get table names
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    print(f"\nTables in database: {[table[0] for table in tables]}")
    
    # Get document count
    cursor.execute("SELECT COUNT(*) FROM documents")
    doc_count = cursor.fetchone()[0]
    print(f"Total documents: {doc_count}")
    
    # Get total character count
    cursor.execute("SELECT SUM(LENGTH(full_text)) FROM documents")
    total_chars = cursor.fetchone()[0]
    if total_chars:
        print(f"Total characters: {total_chars:,}")
    
    # Show sample documents
    cursor.execute("SELECT doc_id, source, episode, episode_title FROM documents LIMIT 3")
    sample_docs = cursor.fetchall()
    print(f"\nSample documents:")
    for doc in sample_docs:
        print(f"  ID: {doc[0]}, Source: {doc[1]}, Episode: {doc[2]}, Title: {doc[3]}")

def search_for_word(conn, word):
    """Search for a specific word in the documents."""
    cursor = conn.cursor()
    
    print(f"\nSearching for word: '{word}'")
    
    # Search using LIKE (case-insensitive in SQLite)
    cursor.execute("""
        SELECT doc_id, source, episode, episode_title, 
               LENGTH(full_text) as text_length,
               (LENGTH(full_text) - LENGTH(REPLACE(LOWER(full_text), LOWER(?), ''))) / LENGTH(?) as word_count
        FROM documents 
        WHERE LOWER(full_text) LIKE LOWER(?)
        ORDER BY word_count DESC
    """, [word, word, f'%{word}%'])
    
    results = cursor.fetchall()
    
    if not results:
        print(f"No documents found containing '{word}'")
        return
    
    print(f"Found {len(results)} documents containing '{word}':")
    
    for i, result in enumerate(results[:5], 1):  # Show first 5 results
        doc_id, source, episode, title, text_length, word_count = result
        print(f"\n{i}. Document ID: {doc_id}")
        print(f"   Source: {source}")
        print(f"   Episode: {episode}")
        print(f"   Title: {title}")
        print(f"   Text length: {text_length:,} characters")
        print(f"   Word count: {word_count}")
        
        # Show context around the word
        cursor.execute("""
            SELECT full_text FROM documents WHERE doc_id = ?
        """, [doc_id])
        
        full_text = cursor.fetchone()[0]
        word_lower = word.lower()
        text_lower = full_text.lower()
        
        # Find first occurrence
        pos = text_lower.find(word_lower)
        if pos != -1:
            start = max(0, pos - 100)
            end = min(len(full_text), pos + len(word) + 100)
            context = full_text[start:end]
            
            # Highlight the word
            word_pos = pos - start
            highlighted_context = (
                context[:word_pos] + 
                f"**{context[word_pos:word_pos + len(word)]}**" + 
                context[word_pos + len(word):]
            )
            
            print(f"   Context: ...{highlighted_context}...")

def search_in_segments(conn, word):
    """Search for a word in the segments table."""
    cursor = conn.cursor()
    
    print(f"\nSearching for word '{word}' in segments:")
    
    cursor.execute("""
        SELECT s.doc_id, s.segment_id, s.segment_text, s.start_time, s.end_time,
               d.source, d.episode, d.episode_title
        FROM segments s
        JOIN documents d ON s.doc_id = d.doc_id
        WHERE LOWER(s.segment_text) LIKE LOWER(?)
        ORDER BY s.doc_id, s.segment_id
        LIMIT 5
    """, [f'%{word}%'])
    
    results = cursor.fetchall()
    
    if not results:
        print(f"No segments found containing '{word}'")
        return
    
    print(f"Found {len(results)} segments containing '{word}':")
    
    for i, result in enumerate(results, 1):
        doc_id, seg_id, text, start_time, end_time, source, episode, title = result
        print(f"\n{i}. Document {doc_id}, Segment {seg_id}")
        print(f"   Source: {source}")
        print(f"   Episode: {episode}")
        print(f"   Title: {title}")
        print(f"   Time: {start_time:.2f}s - {end_time:.2f}s")
        print(f"   Text: {text}")

def main():
    """Main function to demonstrate database queries."""
    print("=== Database Query Demo for Explore App ===\n")
    
    # Connect to database
    conn = connect_to_db()
    if not conn:
        return
    
    try:
        # Show database information
        show_database_info(conn)
        
        # Example words to search for
        example_words = ["example", "the", "and", "hello", "world"]
        
        print(f"\n{'='*50}")
        print("SEARCHING FOR EXAMPLE WORDS")
        print(f"{'='*50}")
        
        for word in example_words:
            search_for_word(conn, word)
            search_in_segments(conn, word)
            print(f"\n{'-'*50}")
        
        # Interactive search
        print(f"\n{'='*50}")
        print("INTERACTIVE SEARCH")
        print(f"{'='*50}")
        
        while True:
            user_word = input("\nEnter a word to search for (or 'quit' to exit): ").strip()
            
            if user_word.lower() in ['quit', 'exit', 'q']:
                break
            
            if user_word:
                search_for_word(conn, user_word)
                search_in_segments(conn, user_word)
            else:
                print("Please enter a word to search for.")
    
    except KeyboardInterrupt:
        print("\n\nSearch interrupted by user.")
    except Exception as e:
        print(f"\nError during search: {e}")
    finally:
        conn.close()
        print("\nDatabase connection closed.")

if __name__ == "__main__":
    main() 