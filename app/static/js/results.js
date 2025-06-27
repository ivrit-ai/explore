// results.js – lightweight client for Explore search results
// ---------------------------------------------------------------
// Assumptions
//   • Every .result-item carries data attributes:
//       data-source   recording_id (url‑encoded)
//       data-epi      episode_idx (int)
//       data-char     char_offset (int)
//       data-seg      segment_idx (int)  – first segment (already known)
//       data-start    start_sec   (float)
//   • A single audio file per recording lives at /audio/<id>.opus
//   • The server exposes:
//       GET /search/segment?episode_idx&char_offset
//       GET /search/segment/by_idx?episode_idx&seg_idx
// ---------------------------------------------------------------

/* ========================
   1 ‑ Audio single‑instance manager
   ======================== */
const audioManager = {
    current: null,
    players: new Map(), // Map to store all audio players

    register(audio, id) {
        this.players.set(id, audio);
        audio.addEventListener('play', () => {
            if (this.current && this.current !== audio) this.current.pause();
            this.current = audio;
        });
        audio.addEventListener('ended', () => { if (this.current === audio) this.current = null; });
        audio.addEventListener('pause', () => { if (this.current === audio) this.current = null; });
        return audio;
    },
    stop() { 
        if (this.current) this.current.pause();
        this.current = null;
    },
    getPlayer(id) {
        return this.players.get(id);
    }
};

/* ========================
   2 ‑ Lazy audio loading queue
   ======================== */
const audioQueue = {
    q: [], active: 0, max: 3,
    add(ph) {
        if (!ph || !ph.isConnected) return;
        this.q.push(ph); this.tick();
    },
    tick() {
        if (this.active >= this.max || !this.q.length) return;
        const ph = this.q.shift();
        this.active++;
        const audio = loadAudio(ph);
        audio.addEventListener('loadedmetadata', () => { this.active--; this.tick(); });
        audio.addEventListener('error',           () => { this.active--; this.tick(); });
    }
};

function loadAudio(placeholder) {
    const srcId = placeholder.dataset.source;
    const fmt   = placeholder.dataset.format || 'opus';
    const start = parseFloat(placeholder.dataset.start) || 0;
    const end = parseFloat(placeholder.dataset.end) || 0;
    // Split srcId at the first slash to get source and filename
    let source = srcId;
    let filename = '';
    if (srcId.includes('/')) {
        const parts = srcId.split('/');
        source = parts[0];
        filename = parts.slice(1).join('/');
    } else {
        filename = srcId;
    }
    const audioUrl = `/audio/${encodeURIComponent(source)}/${encodeURIComponent(filename)}.${fmt}#t=${start}`;
    const playerId = `audio-${srcId}-${start}`;

    const cont  = document.createElement('div'); 
    cont.className = 'audio-container';
    cont.dataset.playerId = playerId;
    
    const audio = document.createElement('audio'); 
    audio.controls = true; 
    audio.preload = 'metadata';
    audio.id = playerId;
    
    const src   = document.createElement('source'); 
    src.src = audioUrl;
    src.type = fmt === 'opus' ? 'audio/ogg; codecs=opus' : fmt === 'mp3' ? 'audio/mpeg' : `audio/${fmt}`;
    
    audio.appendChild(src); 
    cont.appendChild(audio); 
    placeholder.replaceWith(cont);

    audioManager.register(audio, playerId);
    return audio;
}

/* ========================
   3 ‑ Segment fetch helpers
   ======================== */
const segmentCache = {};   // key = `${epi}|${idx}`

// Global batch queue for segment fetches
const segmentBatchQueue = {
    queue: new Map(), // Map<episode_idx, Set<{char_offset, resolve}>>
    timeout: null,
    
    add(epi, char, resolve) {
        if (!this.queue.has(epi)) {
            this.queue.set(epi, new Set());
        }
        this.queue.get(epi).add({ char_offset: char, resolve });
        
        // Schedule a fetch if not already scheduled
        if (!this.timeout) {
            this.timeout = setTimeout(() => this.flush(), 50); // 50ms debounce
        }
    },
    
    async flush() {
        if (this.queue.size === 0) return;
        
        // Convert queue to lookups array
        const lookups = [];
        this.queue.forEach((chars, epi) => {
            chars.forEach(({ char_offset }) => {
                lookups.push({ episode_idx: epi, char_offset });
            });
        });
        
        // Clear queue
        this.queue.clear();
        this.timeout = null;
        
        try {
            const response = await fetch('/search/segment', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ lookups })
            });
            
            const results = await response.json();
            
            // Resolve all promises with their corresponding results
            results.forEach(result => {
                const chars = this.queue.get(result.episode_idx);
                if (chars) {
                    chars.forEach(({ char_offset, resolve }) => {
                        if (char_offset === result.char_offset) {
                            resolve(result);
                        }
                    });
                }
            });
        } catch (error) {
            console.error('Error fetching segments:', error);
            // Reject all pending promises
            this.queue.forEach(chars => {
                chars.forEach(({ resolve }) => resolve(null));
            });
        }
    }
};

function fetchSegmentByChar(epi, char) {
    return new Promise((resolve) => {
        segmentBatchQueue.add(epi, char, resolve);
    });
}

async function fetchSegmentsByIdxBatch(lookups) {
    if (lookups.length === 0) return [];
    
    console.log(`Fetching ${lookups.length} segments by idx in batch`);
    try {
        const response = await fetch('/search/segment/by_idx', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ lookups })
        });
        
        const results = await response.json();
        console.log(`Received ${results.length} segments from batch request`);
        return results;
    } catch (error) {
        console.error('Error fetching segments by idx:', error);
        return [];
    }
}

/* ========================
   4 ‑ Text highlighting utils
   ======================== */
const queryTerm = new URLSearchParams(window.location.search).get('q') || '';
function highlightQuery(txt, charOffset) {
    if (!queryTerm || charOffset === undefined) return txt;
    const escaped = queryTerm.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const regex = new RegExp(`(${escaped})`, 'i');
    const match = txt.slice(charOffset).match(regex);
    if (!match) return txt;
    
    const matchLength = match[0].length;
    return txt.slice(0, charOffset) + 
           `<strong>${txt.slice(charOffset, charOffset + matchLength)}</strong>` + 
           txt.slice(charOffset + matchLength);
}

/* ========================
   5 ‑ Build context & navigation
   ======================== */
function buildContext(resultItem, seg, segmentMap) {
    let curIdx = seg.segment_index;
    const epi = resultItem.dataset.epi;
    const charOffset = parseInt(resultItem.dataset.char);

    const ctx = document.createElement('div');
    ctx.className = 'context-container';
    
    // Get segments before and after from the segmentMap
    const getSegments = () => {
        const segments = [];
        for (let i = curIdx - 5; i <= curIdx + 5; i++) {
            if (i >= 0) {
                const key = `${epi}|${i}`;
                const segment = segmentMap.get(key);
                if (segment) {
                    segments.push(segment);
                }
            }
        }
        return segments;
    };

    // Render all segments
    const renderSegments = (segments) => {
        return segments.map(s => {
            // Only highlight the exact match in the current segment
            const shouldHighlight = s.segment_index === curIdx;
            const text = shouldHighlight ? highlightQuery(s.text, charOffset) : s.text;
            return `
                <div class="context-segment ${s.segment_index === curIdx ? 'current-segment' : ''}"
                     data-start="${s.start_sec}" 
                     data-end="${s.end_sec}"
                     data-seg="${s.segment_index}">
                    ${text}
                </div>
            `;
        }).join('');
    };

    // Initial loading state
    ctx.innerHTML = '<div class="loading">Loading context...</div>';
    
    // Find the result text container and append the context
    const resultTextContainer = resultItem.querySelector('.result-text-container');
    if (resultTextContainer) {
        // Clear any existing content except the audio player and result actions
        const audioPlayer = resultTextContainer.querySelector('.audio-container');
        const resultActions = resultTextContainer.querySelector('.result-actions');
        resultTextContainer.innerHTML = '';
        if (audioPlayer) {
            resultTextContainer.appendChild(audioPlayer);
        }
        if (resultActions) {
            resultTextContainer.appendChild(resultActions);
        }
        resultTextContainer.appendChild(ctx);
    }

    // Get and render segments from the map
    const segments = getSegments();
    ctx.innerHTML = renderSegments(segments);
    
    // Add click handlers to all segments
    ctx.querySelectorAll('.context-segment').forEach(segment => {
        segment.addEventListener('click', e => {
            // Pass the hit index and the clicked segment's start time
            playFromSourceAudio(resultItem.dataset.hitIndex, e.target.dataset.start);
        });
    });

    resultItem.dataset.ctxLoaded = '1';
}

/* ========================
   6 ‑ Audio helper to seek header player
   ======================== */
function playFromSourceAudio(hitIndex, startTime) {
    // Find the result item that matches this hit index
    const resultItem = document.querySelector(`.result-item[data-hit-index="${hitIndex}"]`);
    if (!resultItem) {
        console.warn(`Could not find result item for hit index: ${hitIndex}`);
        return;
    }

    // Get the audio player directly from the result item
    const audio = resultItem.querySelector('audio');
    if (!audio) {
        console.warn(`Could not find audio element in result item`);
        return;
    }

    // Stop all other players
    audioManager.stop();
    
    // Set the start time and load the audio
    audio.currentTime = parseFloat(startTime);
    audio.preload = 'metadata'; // Start loading after setting the time
    
    // Get all segments in the context
    const contextContainer = resultItem.querySelector('.context-container');
    if (!contextContainer) {
        console.warn('Could not find context container');
        return;
    }

    // Find the last segment to get its end time
    const segments = contextContainer.querySelectorAll('.context-segment');
    const lastSegment = segments[segments.length - 1];
    const endTime = parseFloat(lastSegment.dataset.end) || 0;

    // Remove highlighting from all segments
    segments.forEach(seg => seg.classList.remove('playing-segment'));
    
    // Add timeupdate listener to stop at end time and update highlighting
    const timeUpdateHandler = () => {
        // Find the currently playing segment
        let currentSegment = null;
        for (const seg of segments) {
            const segStart = parseFloat(seg.dataset.start);
            const segEnd = parseFloat(seg.dataset.end);
            if (audio.currentTime >= segStart && audio.currentTime < segEnd) {
                currentSegment = seg;
                break;
            }
        }

        // Update highlighting
        segments.forEach(seg => seg.classList.remove('playing-segment'));
        if (currentSegment) {
            currentSegment.classList.add('playing-segment');
        }

        // Stop at end time
        if (audio.currentTime >= endTime) {
            audio.pause();
            audio.currentTime = parseFloat(startTime);
            audio.removeEventListener('timeupdate', timeUpdateHandler);
            // Remove highlighting when stopped
            segments.forEach(seg => seg.classList.remove('playing-segment'));
        }
    };
    audio.addEventListener('timeupdate', timeUpdateHandler);
    
    // Play the audio
    audio.play().catch(err => {
        console.error('Error playing audio:', err);
    });
}

/* ========================
   7 ‑ Result‑item click binding
   ======================== */
document.addEventListener('DOMContentLoaded', () => {
    // Remove any existing source-level audio players
    document.querySelectorAll('.source-header .audio-container').forEach(container => {
        container.remove();
    });

    // Collect all result items first
    const resultItems = Array.from(document.querySelectorAll('.result-item'));
    
    // Create audio players for all results
    resultItems.forEach((item, index) => {
        // Add hit index to the result item
        item.dataset.hitIndex = index;
        
        // Create audio player for this hit
        const srcId = item.dataset.source;
        const start = parseFloat(item.dataset.start) || 0;
        const segIdx = parseInt(item.dataset.segId) || 0;
        const playerId = `audio-hit-${index}`;
        // Split srcId at the first slash to get source and filename
        let source = srcId;
        let filename = '';
        if (srcId.includes('/')) {
            const parts = srcId.split('/');
            source = parts[0];
            filename = parts.slice(1).join('/');
        } else {
            filename = srcId;
        }
        const audioUrl = `/audio/${encodeURIComponent(source)}/${encodeURIComponent(filename)}.opus#t=${start}`;

        const audioContainer = document.createElement('div');
        audioContainer.className = 'audio-container';
        audioContainer.dataset.playerId = playerId;
        audioContainer.dataset.source = srcId;
        audioContainer.dataset.segId = segIdx;

        const audio = document.createElement('audio');
        audio.controls = true;
        audio.preload = 'none'; // Don't preload until we set the start time
        audio.id = playerId;
        audio.dataset.source = srcId;
        audio.dataset.segId = segIdx;

        // Set buffer limits
        audio.addEventListener('loadedmetadata', () => {
            // Set buffer size to 10 seconds or 100KB, whichever is smaller
            const bufferSize = Math.min(10, 100 / (audio.duration * 128)); // 128kbps is typical for opus
            audio.buffered.end = bufferSize;
        });

        const src = document.createElement('source');
        src.src = audioUrl;
        src.type = 'audio/ogg; codecs=opus';
        
        audio.appendChild(src);
        audioContainer.appendChild(audio);

        // Insert the audio player at the start of the result item
        const resultTextContainer = item.querySelector('.result-text-container');
        if (resultTextContainer) {
            resultTextContainer.insertBefore(audioContainer, resultTextContainer.firstChild);
        }

        // Register the audio player
        audioManager.register(audio, playerId);
    });

    // Build lookups array for all results
    const charLookups = resultItems.map(item => ({
        episode_idx: parseInt(item.dataset.epi),
        char_offset: parseInt(item.dataset.char)
    }));

    // First, fetch ALL char-to-segment mappings in one batch
    fetchSegmentsByCharBatch(charLookups).then(segments => {
        // Store segment indices in dataset
        segments.forEach((seg, index) => {
            if (seg) {
                resultItems[index].dataset.segIdx = seg.segment_index;
            }
        });

        // Collect ALL unique episode/segment pairs from ALL results
        const uniqueSegments = new Set();
        segments.forEach(seg => {
            if (seg) {
                // Add the target segment
                uniqueSegments.add(`${seg.episode_idx}|${seg.segment_index}`);
                // Add 5 segments before and after
                for (let i = -5; i <= 5; i++) {
                    const surroundingIdx = seg.segment_index + i;
                    if (surroundingIdx >= 0) {  // Only add non-negative indices
                        uniqueSegments.add(`${seg.episode_idx}|${surroundingIdx}`);
                    }
                }
            }
        });

        // Create ONE master lookup for ALL segments
        const masterLookup = Array.from(uniqueSegments).map(key => {
            const [episode_idx, segment_idx] = key.split('|').map(Number);
            return { episode_idx, segment_idx };
        });

        // Now fetch ALL segments in ONE batch
        return fetchSegmentsByIdxBatch(masterLookup);
    }).then(segmentsByIdx => {
        // Create a map for quick lookup
        const segmentMap = new Map();
        segmentsByIdx.forEach(seg => {
            if (seg) {
                segmentMap.set(`${seg.episode_idx}|${seg.segment_index}`, seg);
            }
        });

        // Build contexts for all results
        resultItems.forEach(item => {
            const epi = item.dataset.epi;
            const segIdx = item.dataset.segIdx;
            if (segIdx) {
                const seg = segmentMap.get(`${epi}|${segIdx}`);
                if (seg) {
                    buildContext(item, seg, segmentMap);
                    item.dataset.ctxLoaded = '1';
                }
            }
        });
    }).catch(error => {
        console.error('Error loading segments:', error);
    });
});

/* ========================
   8 ‑ Lazy audio via IntersectionObserver
   ======================== */
function setupLazyLoading() {
    if (!('IntersectionObserver' in window)) {
        document.querySelectorAll('.audio-placeholder').forEach(p => audioQueue.add(p));
        return;
    }
    const io = new IntersectionObserver(entries => {
        entries.forEach(e => { if (e.isIntersecting) audioQueue.add(e.target); });
    }, { rootMargin: '50px' });
    document.querySelectorAll('.audio-placeholder').forEach(p => io.observe(p));
}

async function fetchSegmentsByCharBatch(lookups) {
    if (lookups.length === 0) return [];
    
    console.log(`Fetching ${lookups.length} segments by char in batch`);
    try {
        const response = await fetch('/search/segment', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ lookups })
        });
        
        const results = await response.json();
        console.log(`Received ${results.length} segments from batch request`);
        return results;
    } catch (error) {
        console.error('Error fetching segments by char:', error);
        return [];
    }
}
