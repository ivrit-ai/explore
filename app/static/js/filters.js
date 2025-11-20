// filters.js – Server-side filtering via URL parameters
// ============================================================

/* ========================
   Filter State Management
   ======================== */
const filterState = {
    allSources: new Map(), // source -> count from metadata
    fullMetadata: null
};

/* ========================
   Get Current URL Parameters
   ======================== */
function getUrlParams() {
    const urlParams = new URLSearchParams(window.location.search);
    return {
        query: urlParams.get('q') || '',
        searchMode: urlParams.get('search_mode') || 'partial',
        dateFrom: urlParams.get('date_from') || '',
        dateTo: urlParams.get('date_to') || '',
        sources: urlParams.get('sources') || '',
        page: urlParams.get('page') || '1',
        maxResults: urlParams.get('max_results_per_page') || '1000'
    };
}

/* ========================
   Fetch Metadata from API
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
   Initialize Filters from URL
   ======================== */
async function initializeFilters() {
    const params = getUrlParams();

    if (!params.query) {
        console.warn('No query found');
        return;
    }

    // Fetch full metadata from API (for populating source checkboxes)
    const metadata = await fetchMetadata(params.query);

    if (metadata && metadata.sources && metadata.date_range) {
        filterState.fullMetadata = metadata;

        // Populate sources map from API data
        filterState.allSources.clear();
        Object.entries(metadata.sources).forEach(([source, count]) => {
            filterState.allSources.set(source, count);
        });

        // Populate UI with full metadata
        populateSourceFilters(params.sources);
        setDateRangeLimits(metadata.date_range);
    }

    // Pre-populate form fields from URL parameters
    populateFormFromUrl(params);

    // Initialize event handlers
    setupFilterEventHandlers();
}

/* ========================
   Populate Form from URL Parameters
   ======================== */
function populateFormFromUrl(params) {
    // Set date inputs
    const dateFromInput = document.getElementById('filter-date-from');
    const dateToInput = document.getElementById('filter-date-to');

    if (dateFromInput && params.dateFrom) {
        dateFromInput.value = params.dateFrom;
    }
    if (dateToInput && params.dateTo) {
        dateToInput.value = params.dateTo;
    }
}

/* ========================
   Populate Source Filter Checkboxes
   ======================== */
function populateSourceFilters(selectedSourcesParam) {
    const sourcesContainer = document.getElementById('filter-sources');
    if (!sourcesContainer) return;

    // Clear loading message
    sourcesContainer.innerHTML = '';

    if (filterState.allSources.size === 0) {
        sourcesContainer.innerHTML = '<div class="filter-loading">לא נמצאו מקורות</div>';
        return;
    }

    // Parse selected sources from URL parameter
    const selectedSources = new Set(
        selectedSourcesParam ? selectedSourcesParam.split(',').map(s => s.trim()) : []
    );

    // If no sources are specified in URL, select all by default
    const selectAll = selectedSources.size === 0;

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
        checkbox.checked = selectAll || selectedSources.has(source);

        const label = document.createElement('label');
        label.htmlFor = checkbox.id;
        label.innerHTML = `
            <span>${source.replace('/', ': ')}</span>
            <span class="filter-source-count">${count}</span>
        `;

        sourceItem.appendChild(checkbox);
        sourceItem.appendChild(label);
        sourcesContainer.appendChild(sourceItem);
    });

    // Update toggle all button state
    updateToggleAllButton();
}

/* ========================
   Set Date Range Input Limits
   ======================== */
function setDateRangeLimits(dateRange) {
    if (!dateRange || !dateRange.min || !dateRange.max) return;

    const dateFromInput = document.getElementById('filter-date-from');
    const dateToInput = document.getElementById('filter-date-to');

    if (dateFromInput) {
        dateFromInput.min = dateRange.min;
        dateFromInput.max = dateRange.max;
    }

    if (dateToInput) {
        dateToInput.min = dateRange.min;
        dateToInput.max = dateRange.max;
    }
}

/* ========================
   Apply Filters (Navigate to URL with parameters)
   ======================== */
function applyFilters() {
    const params = getUrlParams();

    // Get current filter values
    const dateFromInput = document.getElementById('filter-date-from');
    const dateToInput = document.getElementById('filter-date-to');

    const dateFrom = dateFromInput?.value || '';
    const dateTo = dateToInput?.value || '';

    // Get selected sources
    const selectedSources = [];
    document.querySelectorAll('.filter-source-item input[type="checkbox"]:checked').forEach(checkbox => {
        selectedSources.push(checkbox.value);
    });

    // Build URL with filter parameters
    const url = new URL(window.location.href);
    url.searchParams.set('q', params.query);
    url.searchParams.set('search_mode', params.searchMode);
    url.searchParams.set('max_results_per_page', params.maxResults);
    url.searchParams.set('page', '1'); // Reset to page 1 when applying filters

    // Set filter parameters
    if (dateFrom) {
        url.searchParams.set('date_from', dateFrom);
    } else {
        url.searchParams.delete('date_from');
    }

    if (dateTo) {
        url.searchParams.set('date_to', dateTo);
    } else {
        url.searchParams.delete('date_to');
    }

    // Only set sources parameter if not all sources are selected
    if (selectedSources.length > 0 && selectedSources.length < filterState.allSources.size) {
        url.searchParams.set('sources', selectedSources.join(','));
    } else {
        url.searchParams.delete('sources');
    }

    // Navigate to new URL (this will reload the page with filters)
    window.location.href = url.toString();
}

/* ========================
   Clear Filters
   ======================== */
function clearFilters() {
    const params = getUrlParams();

    // Build URL without filter parameters (but keep search mode)
    const url = new URL(window.location.href);
    url.searchParams.set('q', params.query);
    url.searchParams.set('search_mode', params.searchMode);
    url.searchParams.set('max_results_per_page', params.maxResults);
    url.searchParams.set('page', '1');
    url.searchParams.delete('date_from');
    url.searchParams.delete('date_to');
    url.searchParams.delete('sources');

    // Navigate to new URL
    window.location.href = url.toString();
}

/* ========================
   Toggle All Sources
   ======================== */
function toggleAllSources() {
    const checkboxes = document.querySelectorAll('.filter-source-item input[type="checkbox"]');
    if (checkboxes.length === 0) return;

    // Check if all are currently selected
    const allSelected = Array.from(checkboxes).every(cb => cb.checked);

    // Toggle all checkboxes
    checkboxes.forEach(checkbox => {
        checkbox.checked = !allSelected;
    });

    // Update button text
    updateToggleAllButton();
}

function updateToggleAllButton() {
    const button = document.getElementById('toggle-all-sources');
    if (!button) return;

    const checkboxes = document.querySelectorAll('.filter-source-item input[type="checkbox"]');
    if (checkboxes.length === 0) return;

    const allSelected = Array.from(checkboxes).every(cb => cb.checked);
    button.textContent = allSelected ? 'בטל הכל' : 'בחר הכל';
}

/* ========================
   Panel Toggle
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
   Event Handlers Setup
   ======================== */
function setupFilterEventHandlers() {
    // Clear filters button
    const clearBtn = document.getElementById('clear-filters-btn');
    if (clearBtn) {
        clearBtn.addEventListener('click', clearFilters);
    }

    // Toggle all sources button
    const toggleAllBtn = document.getElementById('toggle-all-sources');
    if (toggleAllBtn) {
        toggleAllBtn.addEventListener('click', toggleAllSources);
    }

    // Apply filters button
    const applyBtn = document.getElementById('apply-filters-btn');
    if (applyBtn) {
        applyBtn.addEventListener('click', applyFilters);
    }

    // Panel toggle
    setupPanelToggle();

    // Check if filters are active and show clear button
    updateActiveFiltersIndicator();
}

/* ========================
   Update Active Filters Indicator
   ======================== */
function updateActiveFiltersIndicator() {
    const params = getUrlParams();
    const indicator = document.getElementById('active-filters-indicator');
    const countElement = document.getElementById('active-filters-count');
    const clearBtn = document.getElementById('clear-filters-btn');

    let activeCount = 0;

    // Count active filters
    if (params.dateFrom) activeCount++;
    if (params.dateTo) activeCount++;
    if (params.sources) activeCount++; // Source filter is active if specified

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

/* ========================
   Initialize on DOM Ready
   ======================== */
document.addEventListener('DOMContentLoaded', async () => {
    // Only initialize if we have results
    if (document.querySelectorAll('.source-group').length > 0) {
        await initializeFilters();
    }
});

// Make clearFilters available globally for onclick handlers
window.clearFilters = clearFilters;
