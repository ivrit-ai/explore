import io
import csv
import os
from pydub import AudioSegment
from ..utils import resolve_audio_path

class ExportService:
    def __init__(self, audio_dir=None):
        self.audio_dir = audio_dir

    def export_results_csv(self, results):
        """Export search results to CSV"""
        output = io.StringIO()
        output.write('\ufeff')  # UTF-8 BOM for Excel compatibility
        writer = csv.writer(output, dialect='excel')
        writer.writerow(['Source', 'Text', 'Start Time', 'End Time'])
        
        for result in results:
            text = result['text'].encode('utf-8', errors='replace').decode('utf-8')
            writer.writerow([
                result['source'],
                text,
                result['start'],
                result.get('end', '')
            ])
        
        output.seek(0)
        return output.getvalue()

    def export_audio_segment(self, source, start_time, duration=10, audio_dir=None):
        """Export a segment of an audio file"""
        # Use provided audio_dir or fall back to instance audio_dir
        audio_dir = audio_dir or self.audio_dir
        if not audio_dir:
            raise ValueError("Audio directory not provided")
            
        # Resolve the audio file path
        audio_path = resolve_audio_path(source)
        if not audio_path:
            raise ValueError(f"Audio file not found for source: {source}")
        
        # Load the audio file
        audio = AudioSegment.from_file(audio_path, format="opus")
        
        # Convert times to milliseconds
        start_ms = int(start_time * 1000)
        duration_ms = int(duration * 1000)
        
        # Validate start time
        if start_ms >= len(audio):
            raise ValueError(f"Start time {start_time}s exceeds audio length {len(audio)/1000}s")
        
        # Adjust duration if needed
        if start_ms + duration_ms > len(audio):
            duration_ms = len(audio) - start_ms
        
        # Extract the segment
        segment = audio[start_ms:start_ms + duration_ms]
        
        # Export to buffer
        buffer = io.BytesIO()
        segment.export(buffer, format="opus")
        buffer.seek(0)
        
        return buffer, "opus" 