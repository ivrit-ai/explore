// filters.js – Client-side filtering for search results
// ============================================================

/* ========================
   Filter State Management
   ======================== */
const filterState = {
    sources: new Set(),
    dateFrom: null,
    dateTo: null,
    allSources: new Map(), // source -> count
    totalResults: 0,
    visibleResults: 0,
    fullMetadata: null, // Store full metadata from API
    totalQueryResults: 0 // Total results from search query (all pages)
};

/* ========================
   0 - Fetch Metadata from API
   ======================== */
async function fetchMetadata(query) {
    try {
        const response = await fetch(`/search/metadata?q=${encodeURIComponent(query)}`);
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        const data = await response.json();
        return data;
    } catch (error) {
        console.error('Error fetching metadata:', error);
        return null;
    }
}

/* ========================
   1 - Initialize Filters
   ======================== */
async function initializeFilters() {
    // Get query from URL or page
    const urlParams = new URLSearchParams(window.location.search);
    const query = urlParams.get('q') || '';
    
    if (!query) {
        console.warn('No query found, using current page data only');
        initializeFiltersFromPage();
        return;
    }
    
    // Fetch full metadata from API
    const metadata = await fetchMetadata(query);
    
    if (metadata && metadata.sources && metadata.date_range) {
        // Use full metadata from API
        filterState.fullMetadata = metadata;
        
        // Populate sources map from API data
        filterState.allSources.clear();
        Object.entries(metadata.sources).forEach(([source, count]) => {
            filterState.allSources.set(source, count);
        });
        
        // Set total results from API
        filterState.totalResults = metadata.total_results || 0;
        
        // Populate UI with full metadata
        populateSourceFilters();
        setDateRangeLimits();
    } else {
        // Fallback to current page data if API fails
        console.warn('Failed to fetch metadata, using current page data');
        initializeFiltersFromPage();
    }
    
    // Initialize event handlers
    setupFilterEventHandlers();
    
    // Apply initial filters (will show all since all sources are selected by default)
    applyFilters();
}

function initializeFiltersFromPage() {
    // Extract metadata from all source groups on current page (fallback)
    const sourceGroups = document.querySelectorAll('.source-group');
    filterState.totalResults = sourceGroups.length;
    
    // Build source map with counts
    sourceGroups.forEach(group => {
        const source = group.dataset.source;
        
        if (source) {
            const currentCount = filterState.allSources.get(source) || 0;
            filterState.allSources.set(source, currentCount + 1);
        }
    });
    
    // Populate source filter UI
    populateSourceFilters();
    
    // Set date range limits
    setDateRangeLimits();
}

function populateSourceFilters() {
    const sourcesContainer = document.getElementById('filter-sources');
    if (!sourcesContainer) return;
    
    // Clear loading message
    sourcesContainer.innerHTML = '';
    
    if (filterState.allSources.size === 0) {
        sourcesContainer.innerHTML = '<div class="filter-loading">לא נמצאו מקורות</div>';
        return;
    }
    
    // Sort sources alphabetically
    const sortedSources = Array.from(filterState.allSources.entries()).sort((a, b) => {
        return a[0].localeCompare(b[0], 'he');
    });
    
    // Create checkboxes for each source
    sortedSources.forEach(([source, count]) => {
        const sourceItem = document.createElement('div');
        sourceItem.className = 'filter-source-item';
        
        const checkbox = document.createElement('input');
        checkbox.type = 'checkbox';
        checkbox.id = `filter-source-${source.replace(/[^a-zA-Z0-9]/g, '-')}`;
        checkbox.value = source;
        checkbox.checked = true; // All sources selected by default
        
        const label = document.createElement('label');
        label.htmlFor = checkbox.id;
        label.innerHTML = `
            <span>${source.replace('/', ': ')}</span>
            <span class="filter-source-count">${count}</span>
        `;
        
        sourceItem.appendChild(checkbox);
        sourceItem.appendChild(label);
        sourcesContainer.appendChild(sourceItem);
        
        // Add to filter state (all selected by default)
        filterState.sources.add(source);
    });
}

function setDateRangeLimits() {
    let minDateStr = null;
    let maxDateStr = null;
    
    // Use full metadata if available
    if (filterState.fullMetadata && filterState.fullMetadata.date_range) {
        minDateStr = filterState.fullMetadata.date_range.min;
        maxDateStr = filterState.fullMetadata.date_range.max;
    } else {
        // Fallback: extract from current page
        const sourceGroups = document.querySelectorAll('.source-group');
        const dates = [];
        
        sourceGroups.forEach(group => {
            const dateStr = group.dataset.episodeDate;
            if (dateStr) {
                dates.push(dateStr);
            }
        });
        
        if (dates.length > 0) {
            dates.sort(); // Sort as strings (YYYY-MM-DD format)
            minDateStr = dates[0];
            maxDateStr = dates[dates.length - 1];
        }
    }
    
    if (minDateStr && maxDateStr) {
        const dateFromInput = document.getElementById('filter-date-from');
        const dateToInput = document.getElementById('filter-date-to');
        
        if (dateFromInput) {
            dateFromInput.min = minDateStr;
            dateFromInput.max = maxDateStr;
        }
        
        if (dateToInput) {
            dateToInput.min = minDateStr;
            dateToInput.max = maxDateStr;
        }
    }
}

/* ========================
   2 - Apply Filters
   ======================== */
function applyFilters() {
    // Get current filter values
    const dateFromInput = document.getElementById('filter-date-from');
    const dateToInput = document.getElementById('filter-date-to');
    
    filterState.dateFrom = dateFromInput?.value || null;
    filterState.dateTo = dateToInput?.value || null;
    
    // Get selected sources
    filterState.sources.clear();
    document.querySelectorAll('.filter-source-item input[type="checkbox"]:checked').forEach(checkbox => {
        filterState.sources.add(checkbox.value);
    });
    
    // Apply filters to each source group
    const sourceGroups = document.querySelectorAll('.source-group');
    let visibleCount = 0;
    
    sourceGroups.forEach(group => {
        const source = group.dataset.source;
        const dateStr = group.dataset.episodeDate;
        
        // Check source filter
        const sourceMatch = filterState.sources.size === 0 || filterState.sources.has(source);
        
        // Check date filter
        let dateMatch = true;
        if (dateStr && (filterState.dateFrom || filterState.dateTo)) {
            // Parse dates as YYYY-MM-DD strings for reliable comparison
            const groupDateStr = dateStr.split('T')[0]; // Get just the date part if there's time
            const groupDate = new Date(groupDateStr + 'T00:00:00'); // Normalize to start of day
            
            if (!isNaN(groupDate.getTime())) {
                if (filterState.dateFrom) {
                    const fromDate = new Date(filterState.dateFrom + 'T00:00:00');
                    if (groupDate < fromDate) {
                        dateMatch = false;
                    }
                }
                if (filterState.dateTo) {
                    const toDate = new Date(filterState.dateTo + 'T23:59:59');
                    if (groupDate > toDate) {
                        dateMatch = false;
                    }
                }
            }
        }
        
        // Show or hide group
        if (sourceMatch && dateMatch) {
            group.classList.remove('filtered');
            visibleCount++;
        } else {
            group.classList.add('filtered');
        }
    });
    
    filterState.visibleResults = visibleCount;
    
    // Update UI
    updateFilterStats();
    updateActiveFiltersIndicator();
    updateEmptyState();
}

/* ========================
   3 - Clear Filters
   ======================== */
function clearFilters() {
    // Clear date inputs
    const dateFromInput = document.getElementById('filter-date-from');
    const dateToInput = document.getElementById('filter-date-to');
    
    if (dateFromInput) dateFromInput.value = '';
    if (dateToInput) dateToInput.value = '';
    
    filterState.dateFrom = null;
    filterState.dateTo = null;
    
    // Select all sources
    document.querySelectorAll('.filter-source-item input[type="checkbox"]').forEach(checkbox => {
        checkbox.checked = true;
        filterState.sources.add(checkbox.value);
    });
    
    // Apply filters (will show all)
    applyFilters();
}

/* ========================
   4 - Update UI Feedback
   ======================== */
function updateFilterStats() {
    const statsElement = document.getElementById('filtered-stats');
    if (statsElement) {
        // Count visible source groups on CURRENT PAGE only
        const visibleOnPage = document.querySelectorAll('.source-group:not(.filtered)').length;
        
        // Get total results from query (all pages)
        const totalFromQuery = filterState.totalQueryResults || filterState.totalResults;
        
        // Format: מציג X תוצאות מתוך Y תוצאות לשאילתא
        statsElement.textContent = `מציג ${visibleOnPage} תוצאות מתוך ${totalFromQuery} תוצאות לשאילתא`;
    }
}

function updateActiveFiltersIndicator() {
    const indicator = document.getElementById('active-filters-indicator');
    const countElement = document.getElementById('active-filters-count');
    const clearBtn = document.getElementById('clear-filters-btn');
    
    let activeCount = 0;
    
    // Count date filters
    if (filterState.dateFrom) activeCount++;
    if (filterState.dateTo) activeCount++;
    
    // Count source filters (if not all selected)
    const totalSources = filterState.allSources.size;
    const selectedSources = filterState.sources.size;
    if (selectedSources < totalSources) {
        activeCount++;
    }
    
    if (activeCount > 0) {
        if (indicator) {
            indicator.style.display = 'block';
            if (countElement) {
                countElement.textContent = activeCount;
            }
        }
        if (clearBtn) {
            clearBtn.style.display = 'block';
        }
    } else {
        if (indicator) indicator.style.display = 'none';
        if (clearBtn) clearBtn.style.display = 'none';
    }
}

function updateEmptyState() {
    const emptyState = document.getElementById('filtered-empty-state');
    const resultsContainer = document.querySelector('.results');
    
    if (filterState.visibleResults === 0 && filterState.totalResults > 0) {
        if (emptyState) emptyState.style.display = 'block';
        if (resultsContainer) resultsContainer.style.display = 'none';
    } else {
        if (emptyState) emptyState.style.display = 'none';
        if (resultsContainer) resultsContainer.style.display = 'block';
    }
}

/* ========================
   5 - Panel Toggle
   ======================== */
function setupPanelToggle() {
    const panel = document.getElementById('filter-panel');
    const toggleBtn = document.getElementById('filter-panel-toggle');
    const openBtn = document.getElementById('filter-panel-open');
    
    if (toggleBtn) {
        toggleBtn.addEventListener('click', () => {
            if (panel) {
                panel.classList.remove('open');
            }
        });
    }
    
    if (openBtn) {
        openBtn.addEventListener('click', () => {
            if (panel) {
                panel.classList.add('open');
            }
        });
    }
}

/* ========================
   6 - Event Handlers Setup
   ======================== */
function setupFilterEventHandlers() {
    // Date inputs
    const dateFromInput = document.getElementById('filter-date-from');
    const dateToInput = document.getElementById('filter-date-to');
    
    if (dateFromInput) {
        dateFromInput.addEventListener('input', applyFilters);
    }
    
    if (dateToInput) {
        dateToInput.addEventListener('input', applyFilters);
    }
    
    // Source checkboxes - use event delegation on the container
    // This works even if checkboxes are added dynamically
    const sourcesContainer = document.getElementById('filter-sources');
    if (sourcesContainer) {
        sourcesContainer.addEventListener('change', (e) => {
            // Only handle changes from checkboxes
            if (e.target.type === 'checkbox') {
                applyFilters();
            }
        });
    }
    
    // Clear filters button
    const clearBtn = document.getElementById('clear-filters-btn');
    if (clearBtn) {
        clearBtn.addEventListener('click', clearFilters);
    }
    
    // Panel toggle
    setupPanelToggle();
}

/* ========================
   7 - Initialize on DOM Ready
   ======================== */
document.addEventListener('DOMContentLoaded', async () => {
    // Get total query results from global variable set by template
    if (typeof window.PAGINATION_TOTAL_RESULTS !== 'undefined') {
        filterState.totalQueryResults = window.PAGINATION_TOTAL_RESULTS;
    }
    
    // Only initialize if we have results
    if (document.querySelectorAll('.source-group').length > 0) {
        await initializeFilters();
    }
});

// Make functions available globally for onclick handlers
window.clearFilters = clearFilters;

