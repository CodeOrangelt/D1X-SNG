#!/usr/bin/env python3
"""
DXMA Mission Download API

A REST API for querying mission download locations from DXMA.
Uses file name + checksum combination to handle duplicates.

Endpoints:
- GET /api/missions - Get all missions
- GET /api/missions/<mission_id> - Get specific mission by ID
- GET /api/missions/search?q=<query> - Search missions by title
- GET /api/missions/download/<mission_id> - Get download info for mission
- GET /api/health - Health check
"""

from flask import Flask, jsonify, request, abort, send_file
from flask_cors import CORS
import requests
from bs4 import BeautifulSoup
import re
import hashlib
import os
import time
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from functools import lru_cache
import sqlite3
from datetime import datetime, timedelta
import threading

app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False
CORS(app, resources={r"/*": {"origins": "*"}})  # Allow all origins

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
BASE_URL = "https://sectorgame.com/dxma"
CACHE_DURATION = 3600  # 1 hour cache
DB_FILE = "dxma_cache.db"

# Global session with retry logic
session = None
session_lock = threading.Lock()

def get_session():
    """Get or create HTTP session with retry logic"""
    global session
    with session_lock:
        if session is None:
            session = requests.Session()
            retry_strategy = Retry(
                total=3,
                backoff_factor=0.5,
                status_forcelist=[429, 500, 502, 503, 504],
            )
            adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=20, pool_maxsize=20)
            session.mount("http://", adapter)
            session.mount("https://", adapter)
        return session

def init_db():
    """Initialize SQLite database for caching"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS missions (
            id TEXT PRIMARY KEY,
            title TEXT,
            mode TEXT,
            game TEXT,
            date TEXT,
            author TEXT,
            mission_url TEXT,
            download_url TEXT,
            direct_download_url TEXT,
            file_checksum TEXT,
            file_size INTEGER,
            last_updated TIMESTAMP,
            download_status TEXT
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS download_cache (
            mission_id TEXT PRIMARY KEY,
            filename TEXT,
            checksum TEXT,
            file_size INTEGER,
            download_url TEXT,
            direct_download_url TEXT,
            last_checked TIMESTAMP,
            status TEXT
        )
    ''')
    
    conn.commit()
    conn.close()

def get_file_checksum_and_size(url, timeout=30):
    """Get file checksum and size from download URL"""
    try:
        s = get_session()
        # First try HEAD request to get content length
        head_response = s.head(url, timeout=timeout, allow_redirects=True)
        file_size = head_response.headers.get('content-length')
        file_size = int(file_size) if file_size else None
        
        # Get a partial download to compute checksum (first 1MB should be enough for uniqueness)
        headers = {'Range': 'bytes=0-1048575'} if file_size and file_size > 1048576 else {}
        response = s.get(url, timeout=timeout, headers=headers, stream=True)
        
        if response.status_code not in [200, 206]:
            return None, file_size, f"HTTP {response.status_code}"
        
        # Calculate MD5 checksum of the partial content
        md5_hash = hashlib.md5()
        for chunk in response.iter_content(chunk_size=8192):
            md5_hash.update(chunk)
        
        checksum = md5_hash.hexdigest()
        return checksum, file_size, "success"
        
    except Exception as e:
        logger.error(f"Error getting checksum for {url}: {e}")
        return None, None, str(e)

def get_mission_download_info(mission_id, force_refresh=False):
    """Get download information for a specific mission"""
    
    # Check cache first (unless force refresh)
    if not force_refresh:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('''
            SELECT * FROM download_cache 
            WHERE mission_id = ? AND last_checked > datetime('now', '-1 hour')
        ''', (mission_id,))
        
        cached = cursor.fetchone()
        conn.close()
        
        if cached:
            return {
                'mission_id': cached[0],
                'filename': cached[1],
                'checksum': cached[2],
                'file_size': cached[3],
                'download_url': cached[4],
                'direct_download_url': cached[5],
                'last_checked': cached[6],
                'status': cached[7],
                'cached': True
            }
    
    # Fetch fresh data
    mission_url = f"{BASE_URL}/mission?m={mission_id}"
    
    try:
        s = get_session()
        response = s.get(mission_url, timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Look for the direct download link
        download_link = soup.select_one('a.mission-action-main[download]')
        
        if download_link and download_link.get('href'):
            direct_url = download_link['href']
            if not direct_url.startswith('http'):
                direct_url = f"https://sectorgame.com{direct_url}"
            
            # Extract filename from download attribute or URL
            filename = download_link.get('download', '')
            if not filename:
                filename = direct_url.split('/')[-1].split('?')[0]
            
            # Get checksum and file size
            checksum, file_size, status = get_file_checksum_and_size(direct_url)
            
            result = {
                'mission_id': mission_id,
                'filename': filename,
                'checksum': checksum,
                'file_size': file_size,
                'download_url': f"{BASE_URL}/download?m={mission_id}",
                'direct_download_url': direct_url,
                'last_checked': datetime.now().isoformat(),
                'status': status,
                'cached': False
            }
            
            # Cache the result
            conn = sqlite3.connect(DB_FILE)
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO download_cache 
                (mission_id, filename, checksum, file_size, download_url, direct_download_url, last_checked, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (mission_id, filename, checksum, file_size, result['download_url'], 
                  direct_url, result['last_checked'], status))
            conn.commit()
            conn.close()
            
            return result
        else:
            return {
                'mission_id': mission_id,
                'filename': None,
                'checksum': None,
                'file_size': None,
                'download_url': f"{BASE_URL}/download?m={mission_id}",
                'direct_download_url': None,
                'last_checked': datetime.now().isoformat(),
                'status': 'no_direct_link_found',
                'cached': False
            }
            
    except Exception as e:
        logger.error(f"Error fetching download info for mission {mission_id}: {e}")
        return {
            'mission_id': mission_id,
            'filename': None,
            'checksum': None,
            'file_size': None,
            'download_url': f"{BASE_URL}/download?m={mission_id}",
            'direct_download_url': None,
            'last_checked': datetime.now().isoformat(),
            'status': f'error: {str(e)}',
            'cached': False
        }

@lru_cache(maxsize=100)
def get_missions_page(page=1):
    """Get missions from a specific page (cached)"""
    page_url = f"{BASE_URL}/?page={page}" if page > 1 else BASE_URL
    
    try:
        s = get_session()
        response = s.get(page_url, timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        missions = []
        mission_rows = soup.select('table.missionlist tr')
        
        # Skip header row
        for row in mission_rows[1:]:
            cells = row.select('td')
            if len(cells) < 5:
                continue
                
            title_cell = row.select_one('.column-title')
            if not title_cell:
                continue
            
            mission_link = title_cell.select_one('a')
            if not mission_link:
                continue
            
            mission_url = mission_link['href']
            mission_title = mission_link.text.strip()
            
            # Extract mission ID
            mission_id_match = re.search(r'm=(\d+)', mission_url)
            if not mission_id_match:
                continue
            
            mission_id = mission_id_match.group(1)
            
            # Extract other data
            mode_cell = row.select_one('.column-mode')
            game_cell = row.select_one('.column-game')
            date_cell = row.select_one('.column-date')
            author_cell = row.select_one('.column-user')
            
            mission_data = {
                'id': mission_id,
                'title': mission_title,
                'mode': mode_cell.text.strip() if mode_cell else "",
                'game': game_cell.text.strip() if game_cell else "",
                'date': date_cell.text.strip() if date_cell else "",
                'author': author_cell.select_one('a').text.strip() if author_cell and author_cell.select_one('a') else author_cell.text.strip() if author_cell else "",
                'mission_url': f"{BASE_URL}{mission_url}",
                'download_url': f"{BASE_URL}/download?m={mission_id}"
            }
            missions.append(mission_data)
        
        return missions
        
    except Exception as e:
        logger.error(f"Error fetching page {page}: {e}")
        return []

# API Routes

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'version': '1.0.0'
    })

@app.route('/api/missions', methods=['GET'])
def get_missions():
    """Get all missions with pagination"""
    page = request.args.get('page', 1, type=int)
    limit = request.args.get('limit', 20, type=int)
    
    if page < 1:
        page = 1
    if limit > 100:
        limit = 100
    
    missions = get_missions_page(page)
    
    return jsonify({
        'missions': missions,
        'page': page,
        'limit': limit,
        'count': len(missions),
        'has_more': len(missions) == limit
    })

@app.route('/api/missions/<mission_id>', methods=['GET'])
def get_mission(mission_id):
    """Get specific mission by ID"""
    if not mission_id.isdigit():
        abort(400, description="Mission ID must be numeric")
    
    # Try to find the mission in recent pages
    mission_data = None
    for page in range(1, 6):  # Check first 5 pages
        missions = get_missions_page(page)
        mission_data = next((m for m in missions if m['id'] == mission_id), None)
        if mission_data:
            break
    
    if not mission_data:
        # If not found in recent pages, create basic structure
        mission_data = {
            'id': mission_id,
            'title': f"Mission {mission_id}",
            'mode': "",
            'game': "",
            'date': "",
            'author': "",
            'mission_url': f"{BASE_URL}/mission?m={mission_id}",
            'download_url': f"{BASE_URL}/download?m={mission_id}"
        }
    
    return jsonify(mission_data)

@app.route('/api/missions/search', methods=['GET'])
def search_missions():
    """Search missions by title"""
    query = request.args.get('q', '').lower().strip()
    page = request.args.get('page', 1, type=int)
    
    if not query:
        abort(400, description="Search query 'q' parameter is required")
    
    if page < 1:
        page = 1
    
    # Search through multiple pages
    all_missions = []
    for p in range(1, min(page + 5, 21)):  # Search up to 20 pages
        missions = get_missions_page(p)
        all_missions.extend(missions)
    
    # Filter missions by query
    matching_missions = [
        mission for mission in all_missions
        if query in mission['title'].lower() or 
           query in mission['author'].lower() or
           query in mission['mode'].lower() or
           query in mission['game'].lower()
    ]
    
    return jsonify({
        'missions': matching_missions,
        'query': query,
        'count': len(matching_missions),
        'page': page
    })

@app.route('/api/missions/download/<mission_id>', methods=['GET'])
def get_mission_download(mission_id):
    """Get download information for a mission (with file checksum)"""
    if not mission_id.isdigit():
        abort(400, description="Mission ID must be numeric")
    
    force_refresh = request.args.get('refresh', 'false').lower() == 'true'
    download_info = get_mission_download_info(mission_id, force_refresh=force_refresh)
    
    return jsonify(download_info)

@app.route('/')
def serve_ui():
    """Serve the UI HTML file"""
    return send_file('dxma_ui.html')

@app.errorhandler(404)
def not_found(error):
    # Don't return 404 JSON for favicon requests
    if 'favicon' in request.path:
        return '', 404
    return jsonify({'error': 'Not found'}), 404

@app.errorhandler(400)
def bad_request(error):
    return jsonify({'error': error.description}), 400

@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Internal server error'}), 500

# Initialize database on startup
init_db()

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='DXMA Mission Download API')
    parser.add_argument('--host', default='127.0.0.1', help='Host to bind to')
    parser.add_argument('--port', default=5000, type=int, help='Port to bind to')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    
    args = parser.parse_args()
    
    print(f"Starting DXMA API server on {args.host}:{args.port}")
    print(f"Health check: http://{args.host}:{args.port}/api/health")
    print(f"API Documentation:")
    print(f"  GET /api/missions - Get all missions (paginated)")
    print(f"  GET /api/missions/<id> - Get specific mission")
    print(f"  GET /api/missions/search?q=<query> - Search missions")
    print(f"  GET /api/missions/download/<id> - Get download info with checksum")
    print(f"  GET /api/missions/duplicates - Find duplicate missions by checksum")
    
    app.run(host=args.host, port=args.port, debug=args.debug)