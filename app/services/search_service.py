from .cache_service import load_json_file
import re
import logging
import time
import os
import json
from tqdm import tqdm
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class Segment:
    """Class representing a text segment with start and end positions"""
    start: float = 0
    end: float = 0
    text: str = ""
    
    @classmethod
    def from_dict(cls, segment_dict):
        """Create a Segment object from a dictionary"""
        return cls(
            start=segment_dict.get("start", 0),
            end=segment_dict.get("end", 0),
            text=segment_dict.get("text", "")
        )

class SearchService:
    def __init__(self, file_service):
        self.file_service = file_service
        self.all_segments = {}  # Dictionary to store all segments by source
        self.full_texts = {}    # Dictionary to store full texts by source
        self.index_built = False
        self.last_search_results = {}  # Store complete results of last search for pagination
    
    def build_search_index(self, force_rebuild=False, debug=False):
        """Load all segments into memory for fast searching, and use full texts if available"""
        if self.index_built and not force_rebuild:
            logger.info("Search index already built, skipping")
            return
            
        start_time = time.time()
        logger.info("Building two-phase search index...")
        
        available_files = self.file_service.get_available_files()
        total_segments = 0
        
        # Debug mode: limit to first 3 files
        if debug:
            logger.info("DEBUG MODE: Processing only first 3 files")
            available_files = dict(list(available_files.items())[:3])
        
        # Create a progress bar for all files
        with tqdm(total=len(available_files), desc="Indexing files", unit="file") as pbar:
            # Load all segments from all files
            for source, file_info in available_files.items():
                file_start = time.time()
                
                # Load the JSON file
                json_path = file_info['json_path']
                with open(json_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # Check if the JSON has a top-level "text" field for full text
                if isinstance(data, dict) and "text" in data:
                    # Use the pre-existing full text
                    self.full_texts[source] = data["text"]
                    
                    # Process segments if available using the optimized function
                    segment_count = self.process_segments_data(source, data, show_progress=False)
                    
                    # If no segments were found, create a single segment from the full text
                    if segment_count == 0:
                        self.all_segments[source] = [Segment(0, 0, data["text"])]
                        segment_count = 1
                else:
                    # Handle the case where the JSON is an array of segments
                    if isinstance(data, list):
                        # Create a data structure compatible with process_segments_data
                        wrapped_data = {"segments": data}
                        segment_count = self.process_segments_data(source, wrapped_data, show_progress=False)
                        
                        # Create full text by concatenating all segments
                        if source in self.all_segments:
                            full_text = " ".join([segment.text for segment in self.all_segments[source]])
                            self.full_texts[source] = full_text
                    else:
                        logger.warning(f"Unexpected JSON format in {source}, skipping")
                        pbar.update(1)
                        continue
                
                total_segments += segment_count
                file_time = time.time() - file_start
                
                # Update the progress bar with file info
                pbar.set_postfix(segments=segment_count, time=f"{file_time:.2f}s")
                pbar.update(1)
        
        self.index_built = True
        total_time = time.time() - start_time
        logger.info(f"Two-phase search index built in {total_time:.2f} seconds")
        logger.info(f"Total segments loaded: {total_segments}")
        
        # Print debug information about the index structure if in debug mode
        if debug:
            self._print_debug_index_info()
    
    def _print_debug_index_info(self):
        """Print debug information about the index structure"""
        logger.info("\n===== DEBUG: INDEX STRUCTURE =====")
        
        # Print information about full texts
        logger.info(f"Full texts dictionary contains {len(self.full_texts)} sources")
        for i, (source, text) in enumerate(list(self.full_texts.items())[:2]):
            logger.info(f"  Source: {source}")
            logger.info(f"  Full text sample: {text[:100]}...")
            logger.info("  ---")
        
        # Print information about segments
        logger.info(f"Segments dictionary contains {len(self.all_segments)} sources")
        for i, (source, segments) in enumerate(list(self.all_segments.items())[:2]):
            logger.info(f"  Source: {source}")
            logger.info(f"  Number of segments: {len(segments)}")
            
            # Print a few sample segments
            for j, segment in enumerate(segments[:3]):
                logger.info(f"    Segment {j+1}:")
                logger.info(f"      start: {segment.start}")
                logger.info(f"      end: {segment.end}")
                logger.info(f"      text sample: {segment.text[:50]}...")
            
            logger.info("  ---")
        
        logger.info("===== END DEBUG INFO =====\n")
    
    def search(self, query, use_regex=False, use_substring=False, max_results=100, page=1, progressive=False):
        """
        Two-phase search with pagination and optional progressive loading:
        1. First search in full texts to identify relevant sources
        2. Then search segments only within those matching sources
        3. If progressive=True, return first page results quickly and continue searching
        4. Return paginated results based on page number
        """
        start_time = time.time()
        
        # Build index if not already built
        if not self.index_built:
            logger.info("Index not built yet, building now...")
            self.build_search_index()
        
        # Check if we're requesting a new page of the same search
        search_key = f"{query}_{use_regex}_{use_substring}"
        is_new_search = not self.last_search_results.get('key') == search_key
        
        # If this is a request for a page we've already computed, return it immediately
        if not is_new_search and 'results' in self.last_search_results:
            all_results = self.last_search_results['results']
            
            # If we're still searching progressively and requesting a page beyond what we have,
            # return what we have so far with a flag indicating more results are coming
            if self.last_search_results.get('searching', False) and page > 1:
                current_results_count = len(all_results)
                available_pages = (current_results_count + max_results - 1) // max_results
                
                if page > available_pages:
                    logger.info(f"Requested page {page} but only {available_pages} pages available so far. Still searching...")
                    return {
                        'results': [],
                        'pagination': {
                            'page': page,
                            'total_pages': 0,  # Unknown yet
                            'total_results': current_results_count,
                            'per_page': max_results,
                            'has_next': False,
                            'has_prev': page > 1,
                            'still_searching': True
                        }
                    }
        
        if is_new_search:
            logger.info(f"New search for: '{query}' (regex: {use_regex}, substring: {use_substring}, progressive: {progressive})")
            
            # Phase 1: Identify matching sources from full texts
            matching_sources = self._find_matching_sources(query, use_regex, use_substring)
            logger.info(f"Phase 1 complete: Found {len(matching_sources)} matching sources")
            
            # Initialize results storage
            self.last_search_results = {
                'key': search_key,
                'query': query,
                'results': [],
                'total': 0,
                'searching': progressive
            }
            
            # If progressive loading is enabled and we're requesting the first page
            if progressive and page == 1:
                # Start a background thread to continue searching
                import threading
                
                def background_search():
                    try:
                        logger.info(f"Starting background search for '{query}'")
                        bg_start_time = time.time()
                        
                        # Phase 2: Search within segments of matching sources
                        if use_regex:
                            logger.info("Using regex search strategy")
                            all_results = self._regex_search(query, None, matching_sources)
                        elif use_substring:
                            logger.info("Using substring search strategy")
                            all_results = self._substring_search(query, None, matching_sources)
                        else:
                            logger.info("Using full word search strategy")
                            all_results = self._full_word_search(query, None, matching_sources)
                        
                        # Update the stored results
                        self.last_search_results['results'] = all_results
                        self.last_search_results['total'] = len(all_results)
                        self.last_search_results['searching'] = False
                        
                        bg_search_time = time.time() - bg_start_time
                        logger.info(f"Background search completed in {bg_search_time*1000:.2f}ms, found {len(all_results)} total results")
                    except Exception as e:
                        logger.error(f"Error in background search: {str(e)}")
                        self.last_search_results['searching'] = False
                
                # Get quick first page results
                first_page_results = []
                sources_for_first_page = matching_sources[:min(5, len(matching_sources))]
                
                # Get enough results for the first page
                if use_regex:
                    first_page_results = self._regex_search(query, max_results*2, sources_for_first_page)
                elif use_substring:
                    first_page_results = self._substring_search(query, max_results*2, sources_for_first_page)
                else:
                    first_page_results = self._full_word_search(query, max_results*2, sources_for_first_page)
                
                # Store initial results
                self.last_search_results['results'] = first_page_results
                
                # Start background thread for full search
                thread = threading.Thread(target=background_search)
                thread.daemon = True
                thread.start()
                
                # Return the first page of results immediately
                paginated_results = first_page_results[:max_results]
                
                search_time = time.time() - start_time
                logger.info(f"Initial search completed in {search_time*1000:.2f}ms, returning {len(paginated_results)} results while continuing search")
                
                return {
                    'results': paginated_results,
                    'pagination': {
                        'page': page,
                        'total_pages': 1,  # Unknown yet, at least 1
                        'total_results': len(first_page_results),
                        'per_page': max_results,
                        'has_next': len(first_page_results) > max_results,
                        'has_prev': False,
                        'still_searching': True
                    }
                }
            else:
                # Non-progressive search: get all results
                if use_regex:
                    logger.info("Using regex search strategy")
                    all_results = self._regex_search(query, None, matching_sources)
                elif use_substring:
                    logger.info("Using substring search strategy")
                    all_results = self._substring_search(query, None, matching_sources)
                else:
                    logger.info("Using full word search strategy")
                    all_results = self._full_word_search(query, None, matching_sources)
                
                # Store all results for pagination
                self.last_search_results = {
                    'key': search_key,
                    'query': query,
                    'results': all_results,
                    'total': len(all_results),
                    'searching': False
                }
        else:
            logger.info(f"Fetching page {page} of existing search for: '{query}'")
            all_results = self.last_search_results['results']
        
        # Handle the case where we want all results (no pagination)
        if max_results is None:
            logger.info(f"Returning all {len(all_results)} results (no pagination)")
            return {
                'results': all_results,
                'pagination': {
                    'page': 1,
                    'total_pages': 1,
                    'total_results': len(all_results),
                    'per_page': len(all_results),
                    'has_next': False,
                    'has_prev': False,
                    'still_searching': self.last_search_results.get('searching', False)
                }
            }
        
        # Calculate pagination
        start_idx = (page - 1) * max_results
        end_idx = start_idx + max_results
        
        # Make sure we don't go out of bounds
        if start_idx >= len(all_results):
            paginated_results = []
        else:
            paginated_results = all_results[start_idx:end_idx]
        
        total_results = len(all_results)
        total_pages = max(1, (total_results + max_results - 1) // max_results)  # Ceiling division, min 1 page
        
        search_time = time.time() - start_time
        logger.info(f"Search completed in {search_time*1000:.2f}ms, returning page {page} of {total_pages} ({len(paginated_results)} results)")
        
        # Return paginated results with pagination metadata
        return {
            'results': paginated_results,
            'pagination': {
                'page': page,
                'total_pages': total_pages,
                'total_results': total_results,
                'per_page': max_results,
                'has_next': page < total_pages,
                'has_prev': page > 1,
                'still_searching': self.last_search_results.get('searching', False)
            }
        }
    
    def _find_matching_sources(self, query, use_regex=False, use_substring=False):
        """Find sources with full texts that match the query"""
        matching_sources = []
        
        if use_regex:
            try:
                regex = re.compile(query, re.IGNORECASE)
                for source, full_text in self.full_texts.items():
                    if regex.search(full_text):
                        matching_sources.append(source)
            except re.error as e:
                logger.error(f"Invalid regex pattern: {e}")
                # Fall back to substring matching
                return self._find_matching_sources(query, False, True)
        
        elif use_substring:
            query_lower = query.lower()
            for source, full_text in self.full_texts.items():
                if query_lower in full_text.lower():
                    matching_sources.append(source)
        
        else:
            # Full word search
            pattern = r'\b' + re.escape(query) + r'\b'
            try:
                regex = re.compile(pattern, re.IGNORECASE)
                for source, full_text in self.full_texts.items():
                    if regex.search(full_text):
                        matching_sources.append(source)
            except re.error as e:
                logger.error(f"Invalid regex pattern: {e}")
                # Fall back to substring matching
                return self._find_matching_sources(query, False, True)
        
        return matching_sources
    
    def _substring_search(self, query, max_results=None, matching_sources=None):
        """Simple case-insensitive substring search with optional result limit"""
        results = []
        query_lower = query.lower()
        
        # Track performance by source
        source_times = {}
        
        # If matching_sources is None, search all sources
        sources_to_search = matching_sources if matching_sources is not None else self.all_segments.keys()
        
        for source in sources_to_search:
            if source not in self.all_segments:
                continue
                
            segments = self.all_segments[source]
            source_start = time.time()
            source_results = 0
            
            for i, segment in enumerate(segments):
                if max_results is not None and len(results) >= max_results:
                    logger.info(f"Reached max results ({max_results}), stopping search")
                    break
                
                if query_lower in segment.text.lower():
                    results.append({
                        'start': segment.start,
                        'text': segment.text,
                        'source': source
                    })
                    source_results += 1
            
            source_time = time.time() - source_start
            source_times[source] = {
                'time': source_time,
                'results': source_results,
                'segments': len(segments)
            }
            
            if max_results is not None and len(results) >= max_results:
                break
        
        # Log the slowest sources
        sorted_sources = sorted(source_times.items(), key=lambda x: x[1]['time'], reverse=True)
        if sorted_sources:
            logger.info("Slowest sources in search:")
            for source, stats in sorted_sources[:3]:  # Top 3 slowest
                logger.info(f"  {source}: {stats['time']:.4f}s, {stats['results']} results, {stats['segments']} segments")
        
        return results
    
    def _full_word_search(self, query, max_results=None, matching_sources=None):
        """Search for full word matches only with optional result limit"""
        results = []
        
        # Create a regex pattern that matches the query as a whole word
        # \b represents a word boundary
        pattern = r'\b' + re.escape(query) + r'\b'
        
        try:
            # Compile the regex pattern
            regex = re.compile(pattern, re.IGNORECASE)
            
            # If matching_sources is None, search all sources
            sources_to_search = matching_sources if matching_sources is not None else self.all_segments.keys()
            
            for source in sources_to_search:
                if source not in self.all_segments:
                    continue
                    
                segments = self.all_segments[source]
                
                for segment in segments:
                    if max_results is not None and len(results) >= max_results:
                        logger.info(f"Reached max results ({max_results}), stopping search")
                        break
                    
                    if regex.search(segment.text):
                        results.append({
                            'start': segment.start,
                            'text': segment.text,
                            'source': source
                        })
                
                if max_results is not None and len(results) >= max_results:
                    break
                    
        except re.error as e:
            logger.error(f"Invalid regex pattern: {e}")
            # Fall back to substring search if regex fails
            return self._substring_search(query, max_results, matching_sources)
        
        return results
    
    def _regex_search(self, pattern, max_results=None, matching_sources=None):
        """Search using regex pattern matching with optional result limit"""
        results = []
        
        try:
            # Compile the regex pattern
            regex = re.compile(pattern, re.IGNORECASE)
            
            # If matching_sources is None, search all sources
            sources_to_search = matching_sources if matching_sources is not None else self.all_segments.keys()
            
            for source in sources_to_search:
                if source not in self.all_segments:
                    continue
                    
                segments = self.all_segments[source]
                
                for segment in segments:
                    if max_results is not None and len(results) >= max_results:
                        logger.info(f"Reached max results ({max_results}), stopping search")
                        break
                    
                    if regex.search(segment.text):
                        results.append({
                            'start': segment.start,
                            'text': segment.text,
                            'source': source
                        })
                
                if max_results is not None and len(results) >= max_results:
                    break
                    
        except re.error as e:
            logger.error(f"Invalid regex pattern: {e}")
            # Fall back to literal search if regex is invalid
            return self._substring_search(pattern, max_results, matching_sources)
        
        return results
    
    def _full_scan_search(self, query, max_results=100):
        """Legacy method for compatibility - now just calls substring search"""
        return self._substring_search(query, max_results)
    
    def search_segments(self, query, source_file, available_files, use_substring=False, max_results=100):
        """Search segments in a specific source file with result limit"""
        # If we have the segments already loaded, use them
        if self.index_built and source_file in self.all_segments:
            results = []
            
            if use_substring:
                # Substring search
                query_lower = query.lower()
                for segment in self.all_segments[source_file]:
                    if len(results) >= max_results:
                        break
                    
                    try:
                        if query_lower in segment.text.lower():
                            results.append({
                                'start': segment.start,
                                'text': segment.text,
                                'source': source_file
                            })
                    except Exception as e:
                        logger.error(f"Error processing segment in {source_file}: {e}")
                        continue
            else:
                # Full word search
                pattern = r'\b' + re.escape(query) + r'\b'
                try:
                    regex = re.compile(pattern, re.IGNORECASE)
                    for segment in self.all_segments[source_file]:
                        if len(results) >= max_results:
                            break
                        
                        if regex.search(segment.text):
                            results.append({
                                'start': segment.start,
                                'text': segment.text,
                                'source': source_file
                            })
                except re.error:
                    # Fall back to substring search if regex fails
                    return self.search_segments(query, source_file, available_files, use_substring=True, max_results=max_results)
            
            return results
        
        # Otherwise, load from file
        file_info = available_files[source_file]
        segments = self._get_segments(file_info['json_path'], source_file)
        
        results = []
        
        if use_substring:
            # Substring search
            query_lower = query.lower()
            for segment_dict in segments:
                if len(results) >= max_results:
                    break
                
                try:
                    segment = Segment.from_dict(segment_dict)
                    
                    if query_lower in segment.text.lower():
                        results.append({
                            'start': segment.start,
                            'text': segment.text,
                            'source': source_file
                        })
                except Exception as e:
                    logger.error(f"Error processing segment in {source_file}: {e}")
                    continue
        else:
            # Full word search
            pattern = r'\b' + re.escape(query) + r'\b'
            try:
                regex = re.compile(pattern, re.IGNORECASE)
                for segment_dict in segments:
                    if len(results) >= max_results:
                        break
                    
                    segment = Segment.from_dict(segment_dict)
                    
                    if regex.search(segment.text):
                        results.append({
                            'start': segment.start,
                            'text': segment.text,
                            'source': source_file
                        })
            except re.error:
                # Fall back to substring search if regex fails
                return self.search_segments(query, source_file, available_files, use_substring=True, max_results=max_results)
        
        return results
    
    def _get_segments(self, json_path, source):
        """Load segments from a JSON file"""
        data = load_json_file(json_path)
        if data and 'segments' in data:
            return data['segments']
        return []

    # Function to process segments data
    def process_segments_data(self, source, data, show_progress=False):
        """Process segments data from a JSON file and store only essential fields"""
        if "segments" in data and isinstance(data["segments"], list):
            # Extract only the essential fields (start, end, text) from each segment
            # and create Segment objects instead of dictionaries
            optimized_segments = [
                Segment(
                    start=segment.get("start", 0),
                    end=segment.get("end", 0),
                    text=segment.get("text", "")
                )
                for segment in data["segments"]
            ]
            
            self.all_segments[source] = optimized_segments
            segment_count = len(optimized_segments)
            
            # Only show detailed progress if requested
            if show_progress and segment_count > 100:
                with tqdm(total=segment_count, desc=f"Processing {source}", unit="segment") as segment_pbar:
                    for i, segment in enumerate(optimized_segments):
                        if not segment.text:
                            continue
                        
                        # Update progress bar
                        if i % 10 == 0:
                            segment_pbar.update(10)
            
            return segment_count
        return 0

    def display_top_segments(self, source=None, limit=10):
        """Display the top segments for debugging purposes
        
        Args:
            source: Specific source to show segments from, or None for all sources
            limit: Maximum number of segments to show per source
        """
        if not self.index_built:
            logger.warning("Search index not built yet, nothing to display")
            return
        
        sources_to_display = [source] if source else list(self.all_segments.keys())
        
        for src in sources_to_display:
            if src not in self.all_segments:
                logger.warning(f"Source '{src}' not found in index")
                continue
            
            segments = self.all_segments[src]
            logger.info(f"\n{'='*40}\nTop {min(limit, len(segments))} segments for {src}:")
            
            for i, segment in enumerate(segments[:limit]):
                start_time = segment.start
                end_time = segment.end
                text = segment.text
                
                # Format time as MM:SS
                start_formatted = f"{int(start_time//60):02d}:{int(start_time%60):02d}"
                end_formatted = f"{int(end_time//60):02d}:{int(end_time%60):02d}"
                
                logger.info(f"{i+1}. [{start_formatted}-{end_formatted}] {text[:100]}{'...' if len(text) > 100 else ''}")
            
            logger.info(f"{'='*40}\n")