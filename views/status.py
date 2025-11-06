# views/status.py

from flask import Blueprint, jsonify
from database import get_db, get_cursor

status_bp = Blueprint('status', __name__)

@status_bp.route('/api/status', methods=['GET'])
def get_status():
    """
    Returns the current status of the application and database.
    """
    try:
        conn = get_db()
        cursor = get_cursor(conn)
        cursor.execute("SELECT COUNT(*) FROM contents")
        content_count = cursor.fetchone()[0]
        cursor.close()

        return jsonify({
            'status': 'ok',
            'content_count': content_count
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500
