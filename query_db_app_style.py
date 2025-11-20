#!/usr/bin/env python3
"""
Script to query the database using the app's own database service classes.
This shows how to use the same database connection and query methods as the app.
"""

import sys
import os
from pathlib import Path

# Add the app directory to Python path so we can import the app modules
sys.path.insert(0, str(Path(__file__).parent / "app"))

from services.db import DatabaseService
from services.index import TranscriptIndex

def create_database_service():
    """Create a database service instance like the app does."""
    try:
        # Use SQLite (default for the app)
        db_service = DatabaseService(
            db_type="sqlite",
            path="explore.sqlite"
        )
        print("Successfully created database service")
        return db_service
    except Exception as e:
        print(f"Error creating database service: {e}")
        return None

def show_database_stats(db_service):
    """Show database statistics using the app's TranscriptIndex class."""
    try:
        # Create a TranscriptIndex instance (this is what the app uses)
        index = TranscriptIndex(db_service)
        
        # Get document statistics
        doc_count, total_chars = index.get_document_stats()
        print(f"\nDatabase Statistics:")
        print(f"  Total documents: {doc_count}")
        print(f"  Total characters: {total_chars:,}")
        
        # Get sample documents
        if doc_count > 0:
            print(f"\nSample Documents:")
            for i in range(min(3, doc_count)):
                doc_info = index.get_document_info(i + 1)  # doc_id starts at 1
                print(f"  {i+1}. ID: {doc_info['doc_id']}")
                print(f"     Source: {doc_info['source']}")
                print(f"     Episode: {doc_info['episode']}")
                print(f"     Title: {doc_info['episode_title']}")
                print(f"     Text length: {len(doc_info['full_text']):,} characters")
        
        return index
        
    except Exception as e:
        print(f"Error getting database stats: {e}")
        return None

def search_for_word_app_style(index, word):
    """Search for a word using the app's search method."""
    try:
        print(f"\nSearching for word: '{word}' (using app's search method)")
        
        # Use the app's search method
        hits = index.search_hits(word)
        
        if not hits:
            print(f"No hits found for '{word}'")
            return
        
        print(f"Found {len(hits)} hits for '{word}':")
        
        # Show first few hits
        for i, (doc_id, char_offset) in enumerate(hits[:5], 1):
            print(f"\n{i}. Document {doc_id}, Character offset: {char_offset}")
            
            # Get document info
            doc_info = index.get_document_info(doc_id)
            print(f"   Source: {doc_info['source']}")
            print(f"   Episode: {doc_info['episode']}")
            print(f"   Title: {doc_info['episode_title']}")
            
            # Get the segment containing this hit
            try:
                segment = index.get_segment_at_offset(doc_id, char_offset)
                print(f"   Segment ID: {segment['segment_id']}")
                print(f"   Time: {segment['start_time']:.2f}s - {segment['end_time']:.2f}s")
                print(f"   Text: {segment['segment_text']}")
            except Exception as e:
                print(f"   Could not get segment: {e}")
                
    except Exception as e:
        print(f"Error searching for word '{word}': {e}")

def search_documents_directly(db_service, word):
    """Search documents directly using SQL queries."""
    try:
        print(f"\nSearching for word '{word}' (direct SQL query)")
        
        # Search in documents table
        cursor = db_service.execute("""
            SELECT doc_id, source, episode, episode_title, 
                   LENGTH(full_text) as text_length
            FROM documents 
            WHERE LOWER(full_text) LIKE LOWER(?)
            ORDER BY doc_id
            LIMIT 5
        """, [f'%{word}%'])
        
        results = cursor.fetchall()
        
        if not results:
            print(f"No documents found containing '{word}'")
            return
        
        print(f"Found {len(results)} documents containing '{word}':")
        
        for i, result in enumerate(results, 1):
            doc_id, source, episode, title, text_length = result
            print(f"\n{i}. Document ID: {doc_id}")
            print(f"   Source: {source}")
            print(f"   Episode: {episode}")
            print(f"   Title: {title}")
            print(f"   Text length: {text_length:,} characters")
            
            # Show a snippet of text around the word
            cursor2 = db_service.execute("""
                SELECT full_text FROM documents WHERE doc_id = ?
            """, [doc_id])
            
            full_text = cursor2.fetchone()[0]
            word_lower = word.lower()
            text_lower = full_text.lower()
            
            # Find first occurrence
            pos = text_lower.find(word_lower)
            if pos != -1:
                start = max(0, pos - 50)
                end = min(len(full_text), pos + len(word) + 50)
                context = full_text[start:end]
                
                # Highlight the word
                word_pos = pos - start
                highlighted_context = (
                    context[:word_pos] + 
                    f"**{context[word_pos:word_pos + len(word)]}**" + 
                    context[word_pos + len(word):]
                )
                
                print(f"   Context: ...{highlighted_context}...")
                
    except Exception as e:
        print(f"Error in direct SQL search: {e}")

def main():
    """Main function to demonstrate database queries using app's services."""
    print("=== Database Query Demo (App Style) for Explore App ===\n")
    
    # Create database service
    db_service = create_database_service()
    if not db_service:
        return
    
    try:
        # Show database statistics
        index = show_database_stats(db_service)
        if not index:
            return
        
        # Example words to search for
        example_words = ["example", "the", "and", "hello", "world"]
        
        print(f"\n{'='*60}")
        print("SEARCHING FOR EXAMPLE WORDS USING APP'S METHODS")
        print(f"{'='*60}")
        
        for word in example_words:
            search_for_word_app_style(index, word)
            search_documents_directly(db_service, word)
            print(f"\n{'-'*60}")
        
        # Interactive search
        print(f"\n{'='*60}")
        print("INTERACTIVE SEARCH")
        print(f"{'='*60}")
        
        while True:
            user_word = input("\nEnter a word to search for (or 'quit' to exit): ").strip()
            
            if user_word.lower() in ['quit', 'exit', 'q']:
                break
            
            if user_word:
                search_for_word_app_style(index, user_word)
                search_documents_directly(db_service, user_word)
            else:
                print("Please enter a word to search for.")
    
    except KeyboardInterrupt:
        print("\n\nSearch interrupted by user.")
    except Exception as e:
        print(f"\nError during search: {e}")
    finally:
        db_service.close()
        print("\nDatabase service closed.")

if __name__ == "__main__":
    main() 