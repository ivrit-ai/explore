from flask import Flask, render_template, request, jsonify, send_from_directory, send_file
import json
import time
import os
import csv
from pathlib import Path
import io
from pydub import AudioSegment
from urllib.parse import unquote, quote
from functools import lru_cache
from datetime import datetime, timedelta

app = Flask(__name__)

# Define directories
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
JSON_DIR = os.path.join(BASE_DIR, "data", "json")
AUDIO_DIR = os.path.join(BASE_DIR, "data", "audio")

# Cache configuration
CACHE_DURATION = timedelta(minutes=5)  # How long to keep cache valid
last_scan_time = None
files_cache = {}
segments_cache = {}

def should_refresh_cache():
    """Check if cache needs refreshing"""
    global last_scan_time
    if last_scan_time is None:
        return True
    return datetime.now() - last_scan_time > CACHE_DURATION

@lru_cache(maxsize=128)
def load_json_file(json_path):
    """Load and cache JSON file contents"""
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading JSON file {json_path}: {e}")
        return None

def get_available_files(force_refresh=False):
    """Get list of JSON files and their corresponding audio files with caching"""
    global last_scan_time, files_cache
    
    if not force_refresh and not should_refresh_cache() and files_cache:
        print(f"Using cached file list with {len(files_cache)} files")
        return files_cache
    
    print("\n=== Starting File Scan ===")
    json_files = {}
    
    try:
        # List all items in JSON directory first
        all_items = list(Path(JSON_DIR).iterdir())
        print(f"\nFound {len(all_items)} total items in JSON directory:")
        for item in all_items:
            print(f"  - {item.name}")
        
        for item_path in all_items:
            print(f"\nProcessing: {item_path}")
            base_name = item_path.name
            if item_path.is_file() and base_name.endswith('.json'):
                print(f"  Direct JSON file found: {base_name}")
                base_name = base_name[:-5]
            elif item_path.is_dir():
                print(f"  Directory found: {base_name}")
            else:
                print(f"  Skipping unknown item type: {base_name}")
                continue
            
            # Find JSON file
            json_path = None
            if item_path.is_file():
                json_path = item_path
                print(f"  Using direct JSON file: {json_path}")
            elif item_path.is_dir():
                transcript_path = item_path / "full_transcript.json"
                if transcript_path.exists():
                    json_path = transcript_path
                    print(f"  Found transcript in directory: {json_path}")
                else:
                    print(f"  No full_transcript.json found in directory: {item_path}")
            
            if not json_path:
                print(f"  ✗ No valid JSON found for {base_name}")
                continue
                
            # Find audio file
            audio_path = None
            for ext in ['.ogg', '.mp3']:
                potential_path = Path(AUDIO_DIR) / f"{base_name}{ext}"
                print(f"  Checking for audio: {potential_path}")
                if potential_path.exists():
                    audio_path = potential_path
                    print(f"  ✓ Found matching audio: {audio_path}")
                    break
                    
            if audio_path:
                print(f"  ✓ Adding to available files: {base_name}")
                json_files[base_name] = {
                    'json_path': str(json_path),
                    'audio_path': str(audio_path),
                    'audio_format': audio_path.suffix[1:],
                    'last_modified': os.path.getmtime(json_path)
                }
            else:
                print(f"  ✗ No matching audio file found for {base_name}")
        
        # Update cache
        files_cache = json_files
        last_scan_time = datetime.now()
        
        print(f"\n=== Scan Complete ===")
        print(f"Found {len(json_files)} valid file pairs:")
        for name, info in json_files.items():
            print(f"  - {name}")
            print(f"    JSON: {info['json_path']}")
            print(f"    Audio: {info['audio_path']}")
        
    except Exception as e:
        print(f"Error scanning files: {e}")
        import traceback
        traceback.print_exc()
        if not files_cache:
            return {}
    
    return files_cache

def get_segments(json_path, source):
    """Get segments with caching"""
    global segments_cache
    
    # Check if we have cached segments and they're still valid
    if source in segments_cache:
        cache_entry = segments_cache[source]
        file_mtime = os.path.getmtime(json_path)
        if file_mtime == cache_entry['mtime']:
            print(f"Using cached segments for {source}")
            return cache_entry['segments']
    
    # Load segments from file
    try:
        data = load_json_file(json_path)
        if data and 'segments' in data:
            segments_cache[source] = {
                'segments': data['segments'],
                'mtime': os.path.getmtime(json_path)
            }
            return data['segments']
    except Exception as e:
        print(f"Error loading segments for {source}: {e}")
    
    return []

def search_segments(query, source_file, available_files):
    """Search segments with cached data"""
    print(f"\nSearching in {source_file}:")
    results = []
    file_info = available_files[source_file]
    
    print(f"  Loading segments from: {file_info['json_path']}")
    segments = get_segments(file_info['json_path'], source_file)
    print(f"  Found {len(segments)} total segments")
    
    match_count = 0
    for segment in segments:
        try:
            if query.lower() in str(segment['text']).lower():
                results.append({
                    'start': segment['start'],
                    'text': segment['text'],
                    'source': source_file
                })
                match_count += 1
        except Exception as e:
            print(f"  Error processing segment in {source_file}: {e}")
            continue
    
    print(f"  Found {match_count} matches in this file")
    return results

@app.route('/')
def home():
    return '''
    <html>
        <head>
            <title>ivrit.ai Explore</title>
            <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&display=swap" rel="stylesheet">
            <style>
                * {
                    margin: 0;
                    padding: 0;
                    box-sizing: border-box;
                    font-family: 'Inter', sans-serif;
                }
                
                body {
                    background-color: #f5f7fa;
                    color: #1a1a1a;
                    line-height: 1.6;
                    padding: 2rem;
                }
                
                .container {
                    max-width: 1200px;
                    margin: 0 auto;
                }
                
                h1 {
                    color: #2d3748;
                    margin-bottom: 2rem;
                    font-weight: 600;
                }
                
                .search-form {
                    background: white;
                    padding: 2rem;
                    border-radius: 12px;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                }
                
                input[type="text"] {
                    width: 100%;
                    padding: 1rem;
                    font-size: 1rem;
                    border: 2px solid #e2e8f0;
                    border-radius: 8px;
                    outline: none;
                    transition: border-color 0.2s;
                }
                
                input[type="text"]:focus {
                    border-color: #4299e1;
                }
                
                button {
                    background-color: #4299e1;
                    color: white;
                    padding: 1rem 2rem;
                    border: none;
                    border-radius: 8px;
                    font-size: 1rem;
                    cursor: pointer;
                    transition: background-color 0.2s;
                    margin-top: 1rem;
                }
                
                button:hover {
                    background-color: #3182ce;
                }
            </style>
        </head>
        <body>
            <div class="container">
                <h1>ivrit.ai Explore</h1>
                <form action="/search" method="GET" class="search-form">
                    <input type="text" name="q" placeholder="Enter search term..." autofocus>
                    <button type="submit">Search</button>
                </form>
            </div>
        </body>
    </html>
    '''

@app.route('/check-audio/<filename>')
def check_audio(filename):
    try:
        file_path = os.path.join(AUDIO_DIR, filename)
        if os.path.exists(file_path):
            file_size = os.path.getsize(file_path)
            return f"File exists! Size: {file_size} bytes"
        return "File not found!", 404
    except Exception as e:
        return f"Error: {str(e)}", 500

@app.route('/search')
def search():
    query = request.args.get('q', '')
    print(f"\n=== New Search Request ===")
    print(f"Query: '{query}'")
    
    start_time = time.time()
    all_results = []
    
    available_files = get_available_files()
    selected_files = list(available_files.keys())
    print(f"\nSearching {len(selected_files)} files:")
    for file_name in selected_files:
        print(f"  - {file_name}")
    
    for file_name in selected_files:
        file_info = available_files[file_name]
        print(f"\nProcessing file: {file_name}")
        print(f"  JSON path: {file_info['json_path']}")
        print(f"  Audio path: {file_info['audio_path']}")
        
        results = search_segments(query, file_name, available_files)
        all_results.extend(results)
    
    search_duration = (time.time() - start_time) * 1000
    result_count = len(all_results)
    file_count = len(selected_files)
    
    print(f"\n=== Search Complete ===")
    print(f"Total results: {result_count}")
    print(f"Files searched: {file_count}")
    print(f"Duration: {search_duration:.2f}ms")
    
    if request.headers.get('Accept') == 'application/json':
        return jsonify({
            'results': all_results,
            'stats': {
                'count': result_count,
                'file_count': file_count,
                'duration_ms': round(search_duration, 2)
            }
        })
    
    results_by_source = {}
    if all_results:
        print("\nGrouping results by source:")
        for r in all_results:
            if r['source'] not in results_by_source:
                results_by_source[r['source']] = []
            results_by_source[r['source']].append(r)
        
        # Generate HTML for each source
        results_html = ''
        if all_results:
            # Add export all results button at the top
            results_html += f'''
                <div class="export-controls">
                    <a href="/export/results/{query}" class="export-button">
                        Export All Results to CSV
                    </a>
                </div>
            '''
            
            for source, results in results_by_source.items():
                file_info = available_files[source]
                audio_format = file_info['audio_format']
                encoded_source = quote(source)
                
                results_html += f'''
                    <div class="source-group">
                        <div class="source-header" onclick="toggleSource('{source}')">
                            <span class="toggle-icon" id="icon-{source}">▶</span>
                            <span class="source-title">{source}</span>
                            <span class="result-count">({len(results)} results)</span>
                        </div>
                        <div class="source-exports">
                            <a href="/export/source/{source}?type=json" class="export-button">Export JSON</a>
                            <a href="/export/source/{source}?type=audio" class="export-button">Export Audio</a>
                        </div>
                        <div id="{source}-results" class="source-results" style="display: none;">
                            {''.join(f"""
                                <div class="result-item" data-start="{r['start']}" data-source="{source}">
                                    <div class="navigation-controls">
                                        <button onclick="prevSegment(this)" class="nav-button">← Previous Segment</button>
                                        <button onclick="nextSegment(this)" class="nav-button">Next Segment →</button>
                                        <a href="/export/segment/{source}?start={r['start']}&duration=10" 
                                           class="export-button">Export Segment</a>
                                    </div>
                                    <p class="result-text">{r['text']}</p>
                                    <div class="audio-placeholder" 
                                         data-source="{encoded_source}"
                                         data-format="{audio_format}"
                                         data-start="{r['start']}">
                                    </div>
                                </div>
                            """ for r in results)}
                        </div>
                    </div>
                '''
    else:
        results_html = 'No results found'
        print("No results to display")

    # Enhanced debug info
    debug_info = f'''
        <div class="debug-info" style="margin-top: 20px; padding: 10px; background: #f0f0f0; border: 1px solid #ddd;">
            <h3>Debug Info:</h3>
            <p>Base Directory: {BASE_DIR}</p>
            <p>JSON Directory: {JSON_DIR}</p>
            <p>Audio Directory: {AUDIO_DIR}</p>
            <p>Available files: {list(available_files.keys())}</p>
            <h4>Audio Files in Directory:</h4>
            <ul>
                {''.join(f"<li>{f} - {os.path.exists(os.path.join(AUDIO_DIR, f))}</li>" 
                        for f in os.listdir(AUDIO_DIR))}
            </ul>
        </div>
    '''

    return f'''
    <html>
        <head>
            <title>Search Results</title>
            <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&display=swap" rel="stylesheet">
            <style>
                * {{
                    margin: 0;
                    padding: 0;
                    box-sizing: border-box;
                    font-family: 'Inter', sans-serif;
                }}
                
                body {{
                    background-color: #f5f7fa;
                    color: #1a1a1a;
                    line-height: 1.6;
                    padding: 2rem;
                }}
                
                .container {{
                    max-width: 1200px;
                    margin: 0 auto;
                }}
                
                .header {{
                    margin-bottom: 2rem;
                }}
                
                .back-link {{
                    color: #4299e1;
                    text-decoration: none;
                    display: inline-flex;
                    align-items: center;
                    margin-bottom: 1rem;
                }}
                
                .back-link:hover {{
                    text-decoration: underline;
                }}
                
                h1 {{
                    color: #2d3748;
                    margin-bottom: 1rem;
                    font-weight: 600;
                }}
                
                .stats {{
                    background: white;
                    padding: 1rem;
                    border-radius: 8px;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                    margin-bottom: 2rem;
                    color: #4a5568;
                }}
                
                .source-group {{
                    background: white;
                    border-radius: 12px;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                    margin-bottom: 1.5rem;
                    overflow: hidden;
                }}
                
                .source-header {{
                    padding: 1rem;
                    background: #f8fafc;
                    cursor: pointer;
                    display: flex;
                    align-items: center;
                    gap: 1rem;
                    border-bottom: 1px solid #e2e8f0;
                }}
                
                .source-header:hover {{
                    background: #f1f5f9;
                }}
                
                .source-title {{
                    font-weight: 500;
                    color: #2d3748;
                }}
                
                .result-count {{
                    color: #718096;
                    font-size: 0.9rem;
                }}
                
                .source-exports {{
                    padding: 1rem;
                    background: #f8fafc;
                    display: flex;
                    gap: 0.5rem;
                    border-bottom: 1px solid #e2e8f0;
                }}
                
                .export-button {{
                    background: #4299e1;
                    color: white;
                    text-decoration: none;
                    padding: 0.5rem 1rem;
                    border-radius: 6px;
                    font-size: 0.9rem;
                    transition: background-color 0.2s;
                }}
                
                .export-button:hover {{
                    background: #3182ce;
                }}
                
                .source-results {{
                    padding: 1rem;
                }}
                
                .result-item {{
                    border-bottom: 1px solid #e2e8f0;
                    padding: 1rem 0;
                }}
                
                .result-item:last-child {{
                    border-bottom: none;
                }}
                
                .navigation-controls {{
                    display: flex;
                    gap: 0.5rem;
                    margin-bottom: 1rem;
                }}
                
                .nav-button {{
                    background: #edf2f7;
                    border: none;
                    padding: 0.5rem 1rem;
                    border-radius: 6px;
                    color: #4a5568;
                    cursor: pointer;
                    transition: background-color 0.2s;
                }}
                
                .nav-button:hover {{
                    background: #e2e8f0;
                }}
                
                .result-text {{
                    margin: 1rem 0;
                    color: #2d3748;
                    line-height: 1.8;
                }}
                
                audio {{
                    width: 100%;
                    margin-top: 1rem;
                    border-radius: 8px;
                }}
                
                .export-controls {{
                    margin-bottom: 2rem;
                    text-align: right;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <a href="/" class="back-link">← Back to Search</a>
                    <h1>Search Results for: {query}</h1>
                </div>
                
                <div class="stats">
                    Found {result_count} results in {file_count} files (search took {search_duration:.2f}ms)
                </div>
                
                <div class="results">
                    {results_html}
                </div>
            </div>
            
            <script>
                // Store all segments data for each source
                const sourceSegments = {{
                    {','.join(f"""
                        '{source}': {json.dumps([{'start': seg['start'], 'text': seg['text']} 
                                               for seg in get_segments(available_files[source]['json_path'], source)])}
                    """ for source in results_by_source.keys())}
                }};
                
                function loadAudio(placeholder) {{
                    const source = placeholder.dataset.source;
                    const format = placeholder.dataset.format;
                    const start = placeholder.dataset.start;
                    const audioUrl = `/audio/${{source}}.${{format}}`;
                    
                    const audio = document.createElement('audio');
                    audio.controls = true;
                    audio.preload = "metadata";
                    audio.dataset.currentTime = start;
                    
                    const sourceElement = document.createElement('source');
                    sourceElement.src = `${{audioUrl}}#t=${{start}}`;
                    sourceElement.type = `audio/${{format}}`;
                    
                    audio.appendChild(sourceElement);
                    placeholder.replaceWith(audio);
                    return audio;
                }}
                
                function findSegmentIndex(time, segments) {{
                    return segments.findIndex(seg => Math.abs(seg.start - parseFloat(time)) < 0.1);
                }}
                
                function prevSegment(button) {{
                    const resultItem = button.closest('.result-item');
                    const audioPlaceholder = resultItem.querySelector('.audio-placeholder');
                    const audio = resultItem.querySelector('audio');
                    const source = decodeURIComponent(audioPlaceholder ? audioPlaceholder.dataset.source : resultItem.dataset.source);
                    const currentTime = parseFloat(audioPlaceholder ? audioPlaceholder.dataset.start : audio.dataset.currentTime);
                    
                    const segments = sourceSegments[source];
                    const currentIndex = findSegmentIndex(currentTime, segments);
                    
                    if (currentIndex > 0) {{
                        const prevSegment = segments[currentIndex - 1];
                        updateSegment(resultItem, prevSegment, source);
                    }}
                }}
                
                function nextSegment(button) {{
                    const resultItem = button.closest('.result-item');
                    const audioPlaceholder = resultItem.querySelector('.audio-placeholder');
                    const audio = resultItem.querySelector('audio');
                    const source = decodeURIComponent(audioPlaceholder ? audioPlaceholder.dataset.source : resultItem.dataset.source);
                    const currentTime = parseFloat(audioPlaceholder ? audioPlaceholder.dataset.start : audio.dataset.currentTime);
                    
                    const segments = sourceSegments[source];
                    const currentIndex = findSegmentIndex(currentTime, segments);
                    
                    if (currentIndex < segments.length - 1) {{
                        const nextSegment = segments[currentIndex + 1];
                        updateSegment(resultItem, nextSegment, source);
                    }}
                }}
                
                function updateSegment(resultItem, segment, source) {{
                    const text = resultItem.querySelector('.result-text');
                    text.textContent = segment.text;
                    
                    const audioPlaceholder = resultItem.querySelector('.audio-placeholder');
                    if (audioPlaceholder) {{
                        // Update placeholder data
                        audioPlaceholder.dataset.start = segment.start;
                        // Load audio if not already loaded
                        const audio = loadAudio(audioPlaceholder);
                        audio.currentTime = segment.start;
                        audio.play();
                    }} else {{
                        // Update existing audio
                        const audio = resultItem.querySelector('audio');
                        const sourceElement = audio.querySelector('source');
                        const format = sourceElement.type.split('/')[1];
                        sourceElement.src = `/audio/${{encodeURIComponent(source)}}.${{format}}#t=${{segment.start}}`;
                        audio.load();
                        audio.currentTime = segment.start;
                        audio.play();
                    }}
                }}
                
                function toggleSource(sourceId) {{
                    const resultsDiv = document.getElementById(sourceId + '-results');
                    const icon = document.getElementById('icon-' + sourceId);
                    
                    if (resultsDiv.style.display === 'none') {{
                        resultsDiv.style.display = 'block';
                        icon.textContent = '▼';
                        
                        // Load audio players when section is expanded
                        resultsDiv.querySelectorAll('.audio-placeholder').forEach(placeholder => {{
                            loadAudio(placeholder);
                        }});
                    }} else {{
                        resultsDiv.style.display = 'none';
                        icon.textContent = '▶';
                    }}
                }}
            </script>
        </body>
    </html>
    '''

@app.route('/audio/<path:filename>')
def serve_audio(filename):
    try:
        # Remove any file extension from the filename
        base_name = filename.rsplit('.', 1)[0]
        print(f"Requested audio for base name: {base_name}")
        
        available_files = get_available_files()
        
        # Try to find an exact match first
        if base_name in available_files:
            file_info = available_files[base_name]
            print(f"Found exact match: {file_info['audio_path']}")
            return send_file(file_info['audio_path'])
            
        # If no exact match, try URL-decoded version
        decoded_name = unquote(base_name)
        print(f"Trying decoded name: {decoded_name}")
        
        if decoded_name in available_files:
            file_info = available_files[decoded_name]
            print(f"Found match after decoding: {file_info['audio_path']}")
            return send_file(file_info['audio_path'])
            
        print(f"Available files: {list(available_files.keys())}")
        return f"Audio file not found for {filename}", 404
        
    except Exception as e:
        print(f"Error serving audio file {filename}: {str(e)}")
        import traceback
        traceback.print_exc()
        return f"Error: {str(e)}", 404

@app.route('/export/results/<query>')
def export_results_csv(query):
    available_files = get_available_files()
    all_results = []
    
    for file_name in available_files:
        results = search_segments(query, file_name, available_files)
        all_results.extend(results)
    
    # Create CSV in memory with UTF-8 BOM for Excel compatibility
    output = io.StringIO()
    output.write('\ufeff')  # UTF-8 BOM
    writer = csv.writer(output, dialect='excel')
    writer.writerow(['Source', 'Text', 'Start Time', 'End Time'])
    
    for r in all_results:
        # Ensure text is properly encoded
        text = r['text'].encode('utf-8', errors='replace').decode('utf-8')
        writer.writerow([r['source'], text, r['start'], r.get('end', '')])
    
    # Create the response
    output.seek(0)
    return send_file(
        io.BytesIO(output.getvalue().encode('utf-8')),
        mimetype='text/csv; charset=utf-8',
        as_attachment=True,
        download_name=f'search_results_{query}.csv'
    )

@app.route('/export/source/<source>')
def export_source_files(source):
    available_files = get_available_files()
    if source not in available_files:
        return "Source not found", 404
        
    file_info = available_files[source]
    
    # Determine which file to send based on query parameter
    file_type = request.args.get('type', 'json')
    
    if file_type == 'json':
        return send_file(
            file_info['json_path'],
            mimetype='application/json',
            as_attachment=True,
            download_name=f'{source}.json'
        )
    elif file_type == 'audio':
        return send_file(
            file_info['audio_path'],
            mimetype='audio/ogg',
            as_attachment=True,
            download_name=f'{source}.ogg'
        )

@app.route('/export/segment/<source>')
def export_segment(source):
    start_time = float(request.args.get('start', 0))
    duration = float(request.args.get('duration', 10))
    
    available_files = get_available_files()
    if source not in available_files:
        return "Source not found", 404
    
    try:
        file_info = available_files[source]
        audio_path = file_info['audio_path']
        audio_format = file_info['audio_format']
        
        print(f"Loading audio file: {audio_path}")
        
        if not os.path.exists(audio_path):
            return f"Audio file not found at {audio_path}", 404
            
        if not AudioSegment.ffmpeg:
            return "Error: ffmpeg not found. Please install ffmpeg.", 500
            
        # Load audio file with correct format
        audio = AudioSegment.from_file(audio_path, format=audio_format)
        print(f"Successfully loaded audio file of length {len(audio)}ms")
        
        # Extract segment (convert to milliseconds)
        start_ms = int(start_time * 1000)
        duration_ms = int(duration * 1000)
        
        if start_ms >= len(audio):
            return f"Start time {start_time}s exceeds audio length {len(audio)/1000}s", 400
            
        # Adjust duration if it would exceed file length
        if start_ms + duration_ms > len(audio):
            duration_ms = len(audio) - start_ms
            
        print(f"Extracting segment from {start_ms}ms to {start_ms + duration_ms}ms")
        segment = audio[start_ms:start_ms + duration_ms]
        
        # Export to bytes in original format
        buffer = io.BytesIO()
        segment.export(buffer, format=audio_format)
        buffer.seek(0)
        
        return send_file(
            buffer,
            mimetype=f'audio/{audio_format}',
            as_attachment=True,
            download_name=f'{source}_segment_{start_time:.2f}.{audio_format}'
        )
    except Exception as e:
        print(f"Error in export_segment: {str(e)}")
        import traceback
        traceback.print_exc()
        return f"Error exporting segment: {str(e)}", 500

@app.route('/clear-cache')
def clear_cache():
    """Admin route to clear all caches"""
    global last_scan_time, files_cache, segments_cache
    files_cache = {}
    segments_cache = {}
    last_scan_time = None
    load_json_file.cache_clear()
    return "Cache cleared", 200

if __name__ == '__main__':
    # Initial cache population
    print("Performing initial file scan...")
    get_available_files(force_refresh=True)
    
    # Add ffmpeg check at startup
    try:
        AudioSegment.from_file(os.devnull)
        print("ffmpeg is properly configured")
    except Exception as e:
        print("WARNING: ffmpeg is not properly configured. Audio segment export will not work.")
        print(f"Error: {str(e)}")
        print("Please install ffmpeg and make sure it's in your system PATH")
    
    print("\nFlask server is starting...")
    print(f"Base directory: {BASE_DIR}")
    print(f"JSON directory: {JSON_DIR}")
    print(f"Audio directory: {AUDIO_DIR}")
    
    # Ensure both directories exist
    os.makedirs(JSON_DIR, exist_ok=True)
    os.makedirs(AUDIO_DIR, exist_ok=True)
    
    # List contents of directories with full paths and sizes
    print("\nContents of JSON directory:")
    for f in os.listdir(JSON_DIR):
        path = os.path.join(JSON_DIR, f)
        size = os.path.getsize(path)
        print(f"  {f} - {size} bytes")
    
    print("\nContents of Audio directory:")
    for f in os.listdir(AUDIO_DIR):
        path = os.path.join(AUDIO_DIR, f)
        size = os.path.getsize(path)
        print(f"  {f} - {size} bytes")
        
    app.run(debug=True, port=5000, host='0.0.0.0')
