import os
import json
import uuid
import threading
from datetime import datetime
from flask import Flask, render_template, request, jsonify, send_file, session
from flask_cors import CORS
import pandas as pd
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.secret_key = os.environ.get("SESSION_SECRET", "dev-secret-key-change-in-production")

# Set environment variables for Render
os.environ['PLAYWRIGHT_BROWSERS_PATH'] = '/tmp/ms-playwright'
os.environ['PLAYWRIGHT_DOWNLOAD_HOST'] = 'https://playwright.azureedge.net'

# Set base directory for files
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMP_DIR = os.path.join(BASE_DIR, 'temp')
CHECKPOINT_FILE = os.path.join(BASE_DIR, 'checkpoint_results.csv')

# Create directories if they don't exist
os.makedirs(TEMP_DIR, exist_ok=True)
os.makedirs('/tmp/ms-playwright', exist_ok=True)

# Import background task manager
from background_tasks import TaskManager, create_scraping_task

task_manager = TaskManager(BASE_DIR, TEMP_DIR, CHECKPOINT_FILE)

@app.route('/')
def index():
    """Serve the main page"""
    return render_template('index.html')

@app.route('/api/start_scraping', methods=['POST'])
def start_scraping():
    """Start a new scraping task"""
    try:
        data = request.json
        task_type = data.get('task_type')  # 'poi' or 'search'
        
        # Generate unique task ID
        task_id = str(uuid.uuid4())
        
        # Create and start task
        task = create_scraping_task(task_id, data, BASE_DIR, TEMP_DIR, CHECKPOINT_FILE)
        task_manager.add_task(task_id, task)
        
        # Start task in background thread
        thread = threading.Thread(target=task.run)
        thread.daemon = True
        thread.start()
        
        return jsonify({
            'success': True,
            'task_id': task_id,
            'message': 'Scraping task started in background'
        })
        
    except Exception as e:
        print(f"Start scraping error: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/task_status/<task_id>')
def get_task_status(task_id):
    """Get status of a running task"""
    try:
        task = task_manager.get_task(task_id)
        if not task:
            return jsonify({'error': 'Task not found'}), 404
        
        status = task.get_status()
        return jsonify(status)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/download_results/<task_id>')
def download_results(task_id):
    """Download results as CSV"""
    try:
        # First check if task is still active
        task = task_manager.get_task(task_id)
        
        results_file = None
        
        if task:
            # Task is still in memory, get results file from task
            results_file = task.get_results_file()
        
        # If not found in task, check in temp directory
        if not results_file or not os.path.exists(results_file):
            results_file = os.path.join(TEMP_DIR, f"results_{task_id}.csv")
        
        if not os.path.exists(results_file):
            # Check for checkpoint file as fallback
            if os.path.exists(CHECKPOINT_FILE):
                results_file = CHECKPOINT_FILE
            else:
                # Check for any CSV in temp directory with task_id
                import glob
                temp_files = glob.glob(os.path.join(TEMP_DIR, f"*{task_id}*.csv"))
                if temp_files:
                    results_file = temp_files[0]
                else:
                    return jsonify({
                        'error': 'Results not available yet. Task may still be running or failed.',
                        'task_status': task.get_status() if task else 'No task found'
                    }), 404
        
        # Create a downloadable filename
        filename = f"maps_scraped_{task_id[:8]}.csv"
        
        # Ensure the file is readable
        if not os.access(results_file, os.R_OK):
            return jsonify({'error': 'Results file is not accessible'}), 500
            
        return send_file(
            results_file,
            as_attachment=True,
            download_name=filename,
            mimetype='text/csv'
        )
        
    except Exception as e:
        print(f"Download error: {e}")  # Debug logging
        return jsonify({'error': f'Download failed: {str(e)}'}), 500
        

@app.route('/api/stop_task/<task_id>', methods=['POST'])
def stop_task(task_id):
    """Stop a running task"""
    try:
        task_manager.stop_task(task_id)
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/get_checkpoint')
def get_checkpoint():
    """Get checkpoint data if exists"""
    try:
        # Check multiple possible checkpoint locations
        checkpoint_files = [
            'checkpoint_results.csv',
            'temp/checkpoint_results.csv',
            'checkpoint.csv'
        ]
        
        for filepath in checkpoint_files:
            if os.path.exists(filepath):
                df = pd.read_csv(filepath)
                if not df.empty:
                    return jsonify({
                        'success': True,
                        'data': df.head(100).to_dict('records'),
                        'total_rows': len(df),
                        'file': filepath
                    })
        
        # Also check for any CSV in temp directory
        import glob
        temp_files = glob.glob("temp/*.csv")
        if temp_files:
            # Get the most recent file
            latest_file = max(temp_files, key=os.path.getctime)
            df = pd.read_csv(latest_file)
            return jsonify({
                'success': True,
                'data': df.head(100).to_dict('records'),
                'total_rows': len(df),
                'file': latest_file
            })
        
        return jsonify({
            'success': True,
            'data': [],
            'total_rows': 0,
            'message': 'No checkpoint data found'
        })
        
    except Exception as e:
        print(f"Checkpoint error: {e}")  # Debug logging
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
        

@app.route('/api/clear_checkpoint', methods=['POST'])
def clear_checkpoint():
    """Clear checkpoint data"""
    try:
        if os.path.exists('checkpoint_results.csv'):
            os.remove('checkpoint_results.csv')
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/active_tasks')
def get_active_tasks():
    """Get list of active tasks"""
    tasks = task_manager.get_all_tasks()
    task_list = []
    
    for task_id, task in tasks.items():
        status = task.get_status()
        task_list.append({
            'task_id': task_id,
            'status': status['status'],
            'progress': status['progress'],
            'message': status['message'],
            'started_at': status['started_at'],
            'total_results': status.get('total_results', 0)
        })
    
    return jsonify({'tasks': task_list})

@app.route('/api/debug_tasks')
def debug_tasks():
    """Debug endpoint to see all files and tasks"""
    import glob
    import json
    
    debug_info = {
        'active_tasks': {},
        'temp_files': [],
        'checkpoint_files': []
    }
    
    # Active tasks
    tasks = task_manager.get_all_tasks()
    for task_id, task in tasks.items():
        debug_info['active_tasks'][task_id] = {
            'status': task.status,
            'results_file': task.results_file,
            'file_exists': os.path.exists(task.results_file) if task.results_file else False
        }
    
    # Temp files
    debug_info['temp_files'] = glob.glob("temp/*")
    
    # Checkpoint files
    checkpoint_files = []
    for file in ['checkpoint_results.csv', 'checkpoint.csv', 'results.csv']:
        if os.path.exists(file):
            checkpoint_files.append({
                'name': file,
                'size': os.path.getsize(file),
                'exists': True
            })
    
    debug_info['checkpoint_files'] = checkpoint_files
    
    return jsonify(debug_info)
    

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
