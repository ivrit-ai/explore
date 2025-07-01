import click
from pathlib import Path
from .utils import get_transcripts
from .services.index import IndexManager
import logging
import os
import tempfile

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@click.group()
def cli():
    """Command line interface for managing the transcript index."""
    pass

@cli.command()
@click.argument('data_dir', type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path))
@click.argument('output_file', type=click.Path(dir_okay=False, path_type=Path))
@click.option('--db-type', type=click.Choice(['postgresql', 'sqlite']), 
              default='sqlite', help='Database type to use')
def generate_index(data_dir: Path, output_file: Path, db_type):
    """Generate a flat index file from transcript data.
    
    DATA_DIR: Directory containing transcript files
    OUTPUT_FILE: Path to save the index file (will be gzipped)
    """
    logger.info(f"Generating index from {data_dir} to {output_file} using {db_type}")
    
    # Get transcript records
    file_records = get_transcripts(data_dir)
    
    # Create a temporary database path for index generation
    temp_db_path = tempfile.mktemp(suffix='.db')
    
    index_mgr = IndexManager(
        file_records=file_records, 
        db_type=db_type,
        path=temp_db_path
    )
    
    # Save the index
    index_mgr.save_index(output_file)
    
    # Clean up temporary database file
    try:
        os.remove(temp_db_path)
    except OSError:
        pass  # File might already be gone
    
    logger.info(f"Index saved to {output_file}")

@cli.command()
@click.argument('index_file', type=click.Path(exists=True, file_okay=True, dir_okay=False, path_type=Path))
@click.option('--db-type', type=click.Choice(['postgresql', 'sqlite']), 
              default='sqlite', help='Database type to use')
def validate_index(index_file: Path, db_type):
    """Validate a flat index file.
    
    INDEX_FILE: Path to the index file to validate
    """
    logger.info(f"Validating index file: {index_file} using {db_type}")
    
    try:
        index_mgr = IndexManager(index_path=index_file, db_type=db_type)
        index = index_mgr.get()
        doc_count, total_chars = index.get_document_stats()
        logger.info(f"Index is valid. Contains {doc_count} documents with {total_chars:,} total characters.")
    except Exception as e:
        logger.error(f"Invalid index file: {e}")
        raise click.ClickException(str(e))

if __name__ == '__main__':
    cli() 