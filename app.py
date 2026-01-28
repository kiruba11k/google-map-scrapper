import os
import json
import uuid
import time
import threading
from datetime import datetime
from flask import Flask, render_template, request, jsonify, send_file, session
from flask_cors import CORS
import pandas as pd
from werkzeug.utils import secure_filename
import redis
import pickle

app = Flask(__name__)
app.secret_key = os.environ.get("SESSION_SECRET", "dev-secret-key-change-in-production")
app.config['SESSION_TYPE'] = 'filesystem'
app.config['SESSION_PERMANENT'] = False
CORS(app)

# Redis for session storage (if available, fallback to file system)
try:
    redis_url = os.environ.get("REDIS_URL")
    if redis_url:
        redis_client = redis.from_url(redis_url)
        print("Connected to Redis")
    else:
        redis_client = None
        print("Using file-based session storage")
except:
    redis_client = None

# Import background task manager
from background_tasks import TaskManager, create_scraping_task

task_manager = TaskManager()

# Create temp directory if not exists
os.makedirs('temp', exist_ok=True)

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
        
        # Store task configuration
        if redis_client:
            redis_client.set(f"task_config_{task_id}", json.dumps(data), ex=86400)  # 24h expiry
        else:
            session[f"task_config_{task_id}"] = data
        
        # Create and start task
        task = create_scraping_task(task_id, data)
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
        
        if task:
            # Task is still in memory, get results file from task
            results_file = task.get_results_file()
        else:
            # Task might be completed and cleaned up, check for saved files
            results_file = f"temp/results_{task_id}.csv"
        
        if not results_file or not os.path.exists(results_file):
            # Also check for checkpoint file as fallback
            if os.path.exists('checkpoint_results.csv'):
                results_file = 'checkpoint_results.csv'
            else:
                # Check for any CSV in temp directory with task_id
                import glob
                temp_files = glob.glob(f"temp/*{task_id}*.csv")
                if temp_files:
                    results_file = temp_files[0]
                else:
                    return jsonify({'error': 'Results not available. Task may have failed or not completed.'}), 404
        
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

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
