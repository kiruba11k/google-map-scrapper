// Global variables
let activeTasks = {};
let taskPollingIntervals = {};

// DOM Ready
$(document).ready(function() {
    // Initialize UI elements
    initUI();
    
    // Check for active tasks on page load
    checkActiveTasks();
    
    // Start polling for active tasks
    setInterval(checkActiveTasks, 5000);
});

function initUI() {
    // POI Tab Controls
    $('#auto-poi').change(function() {
        $('#custom-poi').prop('disabled', this.checked);
    });
    
    $('#poi-scroll-delay').on('input', function() {
        $('#poi-delay-value').text($(this).val());
    });
    
    $('#search-scroll-delay').on('input', function() {
        $('#search-delay-value').text($(this).val());
    });
    
    // Start POI Scraping
    $('#start-poi-scraping').click(startPoiScraping);
    
    // Start Search Scraping
    $('#start-search-scraping').click(startSearchScraping);
    
    // Stop buttons
    $('#stop-poi-scraping').click(function() {
        const taskId = $(this).data('task-id');
        if (taskId) stopTask(taskId, 'poi');
    });
    
    $('#stop-search-scraping').click(function() {
        const taskId = $(this).data('task-id');
        if (taskId) stopTask(taskId, 'search');
    });
    
    // Load recovery data
    loadRecoveryData();
    
    // Load active tasks
    loadActiveTasks();
}

function startPoiScraping() {
    const config = {
        task_type: 'poi',
        auto_poi: $('#auto-poi').is(':checked'),
        custom_poi: $('#custom-poi').val(),
        latitude: parseFloat($('#latitude').val()),
        longitude: parseFloat($('#longitude').val()),
        max_results: parseInt($('#poi-max-results').val()),
        scroll_delay: parseFloat($('#poi-scroll-delay').val()),
        mode: $('#poi-mode').val()
    };
    
    // Show progress section
    $('#poi-progress-section').removeClass('d-none');
    $('#poi-progress-bar').css('width', '0%').text('0%');
    $('#poi-progress-message').text('Starting task...');
    
    // Start task
    startTask(config, 'poi');
}

function startSearchScraping() {
    const config = {
        task_type: 'search',
        search_url: $('#search-url').val(),
        max_results: parseInt($('#search-max-results').val()),
        scroll_delay: parseFloat($('#search-scroll-delay').val()),
        mode: $('#search-mode').val()
    };
    
    // Show progress section
    $('#search-progress-section').removeClass('d-none');
    $('#search-progress-bar').css('width', '0%').text('0%');
    $('#search-progress-message').text('Starting task...');
    
    // Start task
    startTask(config, 'search');
}

function startTask(config, taskType) {
    $.ajax({
        url: '/api/start_scraping',
        method: 'POST',
        contentType: 'application/json',
        data: JSON.stringify(config),
        success: function(response) {
            if (response.success) {
                const taskId = response.task_id;
                activeTasks[taskId] = {
                    type: taskType,
                    config: config
                };
                
                // Update UI with task ID
                if (taskType === 'poi') {
                    $('#poi-task-id').text(`Task ID: ${taskId}`);
                    $('#stop-poi-scraping').data('task-id', taskId);
                } else {
                    $('#search-task-id').text(`Task ID: ${taskId}`);
                    $('#stop-search-scraping').data('task-id', taskId);
                }
                
                // Start polling for task status
                startTaskPolling(taskId, taskType);
                
                showNotification('Task started successfully!', 'success');
            } else {
                showNotification('Failed to start task: ' + response.error, 'danger');
            }
        },
        error: function(xhr, status, error) {
            showNotification('Error starting task: ' + error, 'danger');
        }
    });
}

function startTaskPolling(taskId, taskType) {
    // Clear existing interval for this task
    if (taskPollingIntervals[taskId]) {
        clearInterval(taskPollingIntervals[taskId]);
    }
    
    // Start polling
    taskPollingIntervals[taskId] = setInterval(() => {
        pollTaskStatus(taskId, taskType);
    }, 2000);
    
    // Initial poll
    pollTaskStatus(taskId, taskType);
}

function pollTaskStatus(taskId, taskType) {
    $.ajax({
        url: `/api/task_status/${taskId}`,
        method: 'GET',
        success: function(status) {
            if (status.error) {
                // Task not found or error
                clearInterval(taskPollingIntervals[taskId]);
                delete taskPollingIntervals[taskId];
                delete activeTasks[taskId];
                
                if (taskType === 'poi') {
                    $('#poi-progress-message').text('Task not found or error occurred');
                } else {
                    $('#search-progress-message').text('Task not found or error occurred');
                }
                
                return;
            }
            
            // Update UI based on task type
            const progress = Math.round(status.progress * 100);
            
            if (taskType === 'poi') {
                $('#poi-progress-bar').css('width', `${progress}%`).text(`${progress}%`);
                $('#poi-progress-message').html(`
                    <strong>${status.message}</strong><br>
                    <small class="text-muted">Progress: ${progress}% | Results: ${status.total_results || 0}</small>
                `);
            } else {
                $('#search-progress-bar').css('width', `${progress}%`).text(`${progress}%`);
                $('#search-progress-message').html(`
                    <strong>${status.message}</strong><br>
                    <small class="text-muted">Progress: ${progress}% | Results: ${status.total_results || 0}</small>
                `);
            }
            
            // Check if task is complete
            if (status.status === 'completed') {
                clearInterval(taskPollingIntervals[taskId]);
                delete taskPollingIntervals[taskId];
                delete activeTasks[taskId];
                
                // Update badge
                updateActiveTasksBadge();
                
                // Show download button
                showDownloadButton(taskId, status.total_results, taskType);
                
                // Reload recovery data
                loadRecoveryData();
                
                showNotification('Task completed successfully!', 'success');
            } else if (status.status === 'failed') {
                clearInterval(taskPollingIntervals[taskId]);
                delete taskPollingIntervals[taskId];
                delete activeTasks[taskId];
                updateActiveTasksBadge();
                
                showNotification('Task failed: ' + status.message, 'danger');
            } else if (status.status === 'stopped') {
                clearInterval(taskPollingIntervals[taskId]);
                delete taskPollingIntervals[taskId];
                delete activeTasks[taskId];
                updateActiveTasksBadge();
                
                showNotification('Task stopped by user', 'warning');
            }
        },
        error: function(xhr, status, error) {
            console.error('Polling error:', error);
            // Don't clear interval on temporary errors, just log
        }
    });
}
function stopTask(taskId, taskType) {
    $.ajax({
        url: `/api/stop_task/${taskId}`,
        method: 'POST',
        success: function(response) {
            if (response.success) {
                showNotification('Task stop requested', 'warning');
            }
        },
        error: function() {
            showNotification('Error stopping task', 'danger');
        }
    });
}

function showDownloadButton(taskId, resultCount, taskType) {
    const buttonHtml = `
        <div class="alert alert-success mt-3" id="download-section-${taskId}">
            <h6><i class="fas fa-check-circle me-2"></i>Task Completed!</h6>
            <p>Scraped ${resultCount} results.</p>
            <button class="btn btn-primary" onclick="downloadResults('${taskId}')">
                <i class="fas fa-download me-1"></i>Download CSV
            </button>
            <div id="download-status-${taskId}" class="mt-2 small"></div>
        </div>
    `;
    
    if (taskType === 'poi') {
        $('#poi-progress-section').append(buttonHtml);
    } else {
        $('#search-progress-section').append(buttonHtml);
    }
}

function downloadResults(taskId) {
    const statusDiv = $(`#download-status-${taskId}`);
    statusDiv.html('<i class="fas fa-spinner fa-spin me-1"></i> Preparing download...');
    
    // Create a hidden iframe for download
    const iframe = document.createElement('iframe');
    iframe.style.display = 'none';
    iframe.src = `/api/download_results/${taskId}`;
    document.body.appendChild(iframe);
    
    // Check if download started
    setTimeout(() => {
        statusDiv.html('<i class="fas fa-check me-1 text-success"></i> Download started. Check your browser downloads.');
    }, 1000);
    
    // Alternative method using fetch
    /*
    fetch(`/api/download_results/${taskId}`)
        .then(response => {
            if (!response.ok) {
                throw new Error('Download failed');
            }
            return response.blob();
        })
        .then(blob => {
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.style.display = 'none';
            a.href = url;
            a.download = `maps_scraped_${taskId.substring(0, 8)}.csv`;
            document.body.appendChild(a);
            a.click();
            window.URL.revokeObjectURL(url);
            statusDiv.html('<i class="fas fa-check me-1 text-success"></i> Download complete!');
        })
        .catch(error => {
            statusDiv.html(`<i class="fas fa-times me-1 text-danger"></i> ${error.message}`);
        });
    */
}

// Add this to the pollTaskStatus function when task completes:
if (status.status === 'completed') {
    clearInterval(taskPollingIntervals[taskId]);
    delete taskPollingIntervals[taskId];
    delete activeTasks[taskId];
    
    // Update badge
    updateActiveTasksBadge();
    
    // Show download button immediately
    showDownloadButton(taskId, status.total_results, taskType);
    
    // Also create a direct download link
    setTimeout(() => {
        downloadResults(taskId);
    }, 1000);
    
    // Reload recovery data
    loadRecoveryData();
    
    showNotification('Task completed successfully!', 'success');
}
function loadRecoveryData() {
    $.ajax({
        url: '/api/get_checkpoint',
        method: 'GET',
        success: function(response) {
            let html = '';
            
            if (response.success && response.total_rows > 0) {
                html = `
                    <div class="alert alert-success">
                        <h6><i class="fas fa-database me-2"></i>Checkpoint Data Found</h6>
                        <p>Total saved rows: ${response.total_rows}</p>
                        <div class="mt-2">
                            <a href="/api/download_results/checkpoint" class="btn btn-primary btn-sm">
                                <i class="fas fa-download me-1"></i>Download Checkpoint
                            </a>
                            <button class="btn btn-danger btn-sm ms-2" onclick="clearCheckpoint()">
                                <i class="fas fa-trash me-1"></i>Clear Checkpoint
                            </button>
                        </div>
                    </div>
                `;
                
                if (response.data && response.data.length > 0) {
                    html += `
                        <div class="table-responsive mt-3">
                            <table class="table table-striped table-sm">
                                <thead>
                                    <tr>
                                        <th>Name</th>
                                        <th>Rating</th>
                                        <th>Reviews</th>
                                        <th>Industry</th>
                                        <th>Status</th>
                                    </tr>
                                </thead>
                                <tbody>
                    `;
                    
                    response.data.forEach(row => {
                        html += `
                            <tr>
                                <td>${row.name || ''}</td>
                                <td>${row.rating || ''}</td>
                                <td>${row.reviews || ''}</td>
                                <td>${row.industry || ''}</td>
                                <td><span class="badge bg-success">${row.status || 'ok'}</span></td>
                            </tr>
                        `;
                    });
                    
                    html += `
                                </tbody>
                            </table>
                        </div>
                        <p class="small text-muted">Showing first 100 rows of ${response.total_rows} total</p>
                    `;
                }
            } else {
                html = `
                    <div class="alert alert-info">
                        <i class="fas fa-info-circle me-2"></i>
                        No checkpoint data found. Start a scraping task to create checkpoint data.
                    </div>
                `;
            }
            
            $('#recovery-content').html(html);
        },
        error: function() {
            $('#recovery-content').html(`
                <div class="alert alert-danger">
                    <i class="fas fa-exclamation-triangle me-2"></i>
                    Error loading checkpoint data.
                </div>
            `);
        }
    });
}

function clearCheckpoint() {
    if (confirm('Are you sure you want to clear all checkpoint data?')) {
        $.ajax({
            url: '/api/clear_checkpoint',
            method: 'POST',
            success: function(response) {
                if (response.success) {
                    showNotification('Checkpoint cleared successfully', 'success');
                    loadRecoveryData();
                }
            },
            error: function() {
                showNotification('Error clearing checkpoint', 'danger');
            }
        });
    }
}

function loadActiveTasks() {
    $.ajax({
        url: '/api/active_tasks',
        method: 'GET',
        success: function(response) {
            let html = '';
            
            if (response.tasks && response.tasks.length > 0) {
                html = `
                    <div class="list-group">
                `;
                
                response.tasks.forEach(task => {
                    const progress = Math.round(task.progress * 100);
                    let badgeClass = 'bg-primary';
                    
                    if (task.status === 'completed') badgeClass = 'bg-success';
                    else if (task.status === 'failed') badgeClass = 'bg-danger';
                    else if (task.status === 'stopped') badgeClass = 'bg-warning';
                    
                    html += `
                        <div class="list-group-item">
                            <div class="d-flex w-100 justify-content-between">
                                <h6 class="mb-1">Task: ${task.task_id.substring(0, 8)}...</h6>
                                <span class="badge ${badgeClass}">${task.status}</span>
                            </div>
                            <p class="mb-1">${task.message}</p>
                            <small class="text-muted">Started: ${new Date(task.started_at).toLocaleString()}</small>
                            <div class="progress mt-2" style="height: 10px;">
                                <div class="progress-bar" role="progressbar" 
                                     style="width: ${progress}%">${progress}%</div>
                            </div>
                            <div class="mt-2">
                                <button class="btn btn-sm btn-outline-danger" onclick="stopTask('${task.task_id}')">
                                    <i class="fas fa-stop me-1"></i>Stop
                                </button>
                                ${task.status === 'completed' ? `
                                    <a href="/api/download_results/${task.task_id}" class="btn btn-sm btn-outline-success ms-1">
                                        <i class="fas fa-download me-1"></i>Download
                                    </a>
                                ` : ''}
                            </div>
                        </div>
                    `;
                });
                
                html += `</div>`;
            } else {
                html = `
                    <div class="alert alert-info">
                        <i class="fas fa-info-circle me-2"></i>
                        No active tasks. Start a new scraping task to see it here.
                    </div>
                `;
            }
            
            $('#tasks-content').html(html);
        },
        error: function() {
            $('#tasks-content').html(`
                <div class="alert alert-danger">
                    <i class="fas fa-exclamation-triangle me-2"></i>
                    Error loading active tasks.
                </div>
            `);
        }
    });
}

function checkActiveTasks() {
    $.ajax({
        url: '/api/active_tasks',
        method: 'GET',
        success: function(response) {
            const activeCount = response.tasks ? response.tasks.length : 0;
            updateActiveTasksBadge(activeCount);
            
            // If on tasks tab, refresh
            if ($('#tasks-tab').hasClass('active')) {
                loadActiveTasks();
            }
        }
    });
}

function updateActiveTasksBadge(count) {
    const badge = $('#active-tasks-badge');
    const countSpan = $('#active-tasks-count');
    
    if (count > 0) {
        countSpan.text(count);
        badge.removeClass('d-none');
    } else {
        badge.addClass('d-none');
    }
}

function showNotification(message, type) {
    // Remove existing notifications
    $('.alert-notification').remove();
    
    // Create new notification
    const alertClass = type === 'success' ? 'alert-success' : 
                      type === 'danger' ? 'alert-danger' :
                      type === 'warning' ? 'alert-warning' : 'alert-info';
    
    const notification = $(`
        <div class="alert ${alertClass} alert-dismissible fade show alert-notification" role="alert">
            ${message}
            <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
        </div>
    `);
    
    // Add to page
    $('.container').prepend(notification);
    
    // Auto-remove after 5 seconds
    setTimeout(() => {
        notification.alert('close');
    }, 5000);
}

// Export for use in console
window.stopTask = stopTask;
window.clearCheckpoint = clearCheckpoint;
