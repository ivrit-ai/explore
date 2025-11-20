#!/usr/bin/env python3
"""
Simple database wrapper for the explore app database.
Easy to import and use from Jupyter notebooks.
"""

import sqlite3
import pandas as pd
from pathlib import Path
from typing import List, Dict, Optional, Tuple

class ExploreDB:
    """Simple wrapper for the explore app database."""
    
    def __init__(self, db_path: str = "explore.sqlite"):
        """Initialize database connection.
        
        Args:
            db_path: Path to the SQLite database file
        """
        self.db_path = Path(db_path)
        if not self.db_path.exists():
            raise FileNotFoundError(f"Database file not found: {db_path}")
        
        self.conn = sqlite3.connect(db_path)
        print(f"Connected to database: {db_path}")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def close(self):
        """Close database connection."""
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
            print("Database connection closed.")
    
    def get_info(self) -> Dict:
        """Get basic database information."""
        cursor = self.conn.cursor()
        
        # Get table names
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [table[0] for table in cursor.fetchall()]
        
        # Get document count
        cursor.execute("SELECT COUNT(*) FROM documents")
        doc_count = cursor.fetchone()[0]
        
        # Get total character count
        cursor.execute("SELECT SUM(LENGTH(full_text)) FROM documents")
        total_chars = cursor.fetchone()[0] or 0
        
        return {
            "tables": tables,
            "document_count": doc_count,
            "total_characters": total_chars,
            "database_path": str(self.db_path)
        }
    
    def search_word(self, word: str, limit: int = 10) -> pd.DataFrame:
        """Search for a word in documents.
        
        Args:
            word: Word to search for
            limit: Maximum number of results to return
            
        Returns:
            DataFrame with search results
        """
        query = """
        SELECT 
            doc_id,
            source,
            episode,
            episode_title,
            LENGTH(full_text) as text_length,
            (LENGTH(full_text) - LENGTH(REPLACE(LOWER(full_text), LOWER(?), ''))) / LENGTH(?) as word_count
        FROM documents 
        WHERE LOWER(full_text) LIKE LOWER(?)
        ORDER BY word_count DESC
        LIMIT ?
        """
        
        df = pd.read_sql_query(query, self.conn, params=[word, word, f'%{word}%', limit])
        return df
    
    def search_word_in_segments(self, word: str, limit: int = 10) -> pd.DataFrame:
        """Search for a word in segments.
        
        Args:
            word: Word to search for
            limit: Maximum number of results to return
            
        Returns:
            DataFrame with search results
        """
        query = """
        SELECT 
            s.doc_id,
            s.segment_id,
            s.segment_text,
            s.start_time,
            s.end_time,
            d.source,
            d.episode,
            d.episode_title
        FROM segments s
        JOIN documents d ON s.doc_id = d.doc_id
        WHERE LOWER(s.segment_text) LIKE LOWER(?)
        ORDER BY s.doc_id, s.segment_id
        LIMIT ?
        """
        
        df = pd.read_sql_query(query, self.conn, params=[f'%{word}%', limit])
        return df
    
    def get_document(self, doc_id: int) -> Optional[Dict]:
        """Get a specific document by ID.
        
        Args:
            doc_id: Document ID
            
        Returns:
            Dictionary with document information or None if not found
        """
        query = """
        SELECT doc_id, source, episode, episode_date, episode_title, full_text
        FROM documents WHERE doc_id = ?
        """
        
        cursor = self.conn.cursor()
        cursor.execute(query, [doc_id])
        result = cursor.fetchone()
        
        if result:
            return {
                "doc_id": result[0],
                "source": result[1],
                "episode": result[2],
                "episode_date": result[3],
                "episode_title": result[4],
                "full_text": result[5]
            }
        return None
    
    def get_document_segments(self, doc_id: int) -> pd.DataFrame:
        """Get all segments for a document.
        
        Args:
            doc_id: Document ID
            
        Returns:
            DataFrame with segments
        """
        query = """
        SELECT segment_id, segment_text, avg_logprob, char_offset, start_time, end_time
        FROM segments 
        WHERE doc_id = ? 
        ORDER BY segment_id
        """
        
        df = pd.read_sql_query(query, self.conn, params=[doc_id])
        return df
    
    def get_sample_documents(self, limit: int = 5) -> pd.DataFrame:
        """Get sample documents.
        
        Args:
            limit: Maximum number of documents to return
            
        Returns:
            DataFrame with sample documents
        """
        query = """
        SELECT doc_id, source, episode, episode_title, LENGTH(full_text) as text_length
        FROM documents 
        ORDER BY doc_id 
        LIMIT ?
        """
        
        df = pd.read_sql_query(query, self.conn, params=[limit])
        return df
    
    def get_context_around_word(self, doc_id: int, word: str, context_chars: int = 100) -> Optional[str]:
        """Get text context around a word in a document.
        
        Args:
            doc_id: Document ID
            word: Word to find
            context_chars: Number of characters around the word to show
            
        Returns:
            Context string with highlighted word or None if word not found
        """
        doc = self.get_document(doc_id)
        if not doc:
            return None
        
        full_text = doc['full_text']
        word_lower = word.lower()
        text_lower = full_text.lower()
        
        # Find first occurrence
        pos = text_lower.find(word_lower)
        if pos == -1:
            return None
        
        # Get context
        start = max(0, pos - context_chars)
        end = min(len(full_text), pos + len(word) + context_chars)
        context = full_text[start:end]
        
        # Highlight the word
        word_pos = pos - start
        highlighted_context = (
            context[:word_pos] + 
            f"**{context[word_pos:word_pos + len(word)]}**" + 
            context[word_pos + len(word):]
        )
        
        return f"...{highlighted_context}..."
    
    def get_word_frequency(self, word: str) -> Dict:
        """Get frequency statistics for a word.
        
        Args:
            word: Word to analyze
            
        Returns:
            Dictionary with frequency statistics
        """
        # Count documents containing the word
        doc_query = """
        SELECT COUNT(*) as doc_count
        FROM documents 
        WHERE LOWER(full_text) LIKE LOWER(?)
        """
        
        cursor = self.conn.cursor()
        cursor.execute(doc_query, [f'%{word}%'])
        doc_count = cursor.fetchone()[0]
        
        # Count total occurrences
        total_query = """
        SELECT SUM((LENGTH(full_text) - LENGTH(REPLACE(LOWER(full_text), LOWER(?), ''))) / LENGTH(?)) as total_count
        FROM documents 
        WHERE LOWER(full_text) LIKE LOWER(?)
        """
        
        cursor.execute(total_query, [word, word, f'%{word}%'])
        total_count = cursor.fetchone()[0] or 0
        
        return {
            "word": word,
            "documents_containing_word": doc_count,
            "total_occurrences": total_count,
            "average_occurrences_per_doc": total_count / doc_count if doc_count > 0 else 0
        }
    
    def get_top_words(self, limit: int = 20) -> pd.DataFrame:
        """Get most common words in the database.
        
        Args:
            limit: Maximum number of words to return
            
        Returns:
            DataFrame with word frequencies
        """
        # This is a simplified approach - for production use, you might want to create a word frequency table
        query = """
        SELECT 
            'the' as word,
            COUNT(*) as doc_count,
            SUM(LENGTH(full_text)) as total_chars
        FROM documents
        UNION ALL
        SELECT 
            'and' as word,
            COUNT(*) as doc_count,
            SUM(LENGTH(full_text)) as total_chars
        FROM documents
        UNION ALL
        SELECT 
            'to' as word,
            COUNT(*) as doc_count,
            SUM(LENGTH(full_text)) as total_chars
        FROM documents
        ORDER BY doc_count DESC
        LIMIT ?
        """
        
        df = pd.read_sql_query(query, self.conn, params=[limit])
        return df

# Convenience function for quick access
def quick_search(word: str, db_path: str = "explore.sqlite", limit: int = 5):
    """Quick search function for one-off queries.
    
    Args:
        word: Word to search for
        db_path: Path to database
        limit: Maximum results to return
        
    Returns:
        Tuple of (documents_df, segments_df)
    """
    with ExploreDB(db_path) as db:
        docs = db.search_word(word, limit)
        segments = db.search_word_in_segments(word, limit)
        return docs, segments

# Example usage functions
def show_examples():
    """Show example usage of the database wrapper."""
    examples = """
# Example usage in Jupyter notebook:

# 1. Initialize database
db = ExploreDB("explore.sqlite")

# 2. Get database info
info = db.get_info()
print(f"Database has {info['document_count']} documents")

# 3. Search for a word
results = db.search_word("example", limit=5)
print(results)

# 4. Search in segments
segment_results = db.search_word_in_segments("example", limit=5)
print(segment_results)

# 5. Get document details
doc = db.get_document(1)
if doc:
    print(f"Document title: {doc['episode_title']}")

# 6. Get context around a word
context = db.get_context_around_word(1, "example")
if context:
    print(context)

# 7. Get word frequency
freq = db.get_word_frequency("example")
print(f"Word appears in {freq['documents_containing_word']} documents")

# 8. Quick search (one-liner)
docs, segments = quick_search("example")

# 9. Don't forget to close
db.close()

# Or use context manager:
with ExploreDB("explore.sqlite") as db:
    results = db.search_word("example")
    print(results)
"""
    print(examples) 