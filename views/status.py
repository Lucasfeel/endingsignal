# views/status.py

from flask import Blueprint, jsonify, current_app
from database import get_db, get_cursor

status_bp = Blueprint('status', __name__)

@status_bp.route('/api/status', methods=['GET'])
def get_status():
    """
    Returns the current status of the application and database.
    """
    cursor = None
    try:
        conn = get_db()
        cursor = get_cursor(conn)
        cursor.execute("SELECT COUNT(*) as count FROM contents")
        content_count = cursor.fetchone()['count']

        return jsonify({
            'status': 'ok',
            'content_count': content_count
        })
    except Exception:
        current_app.logger.exception("Status check failed")
        return jsonify({
            'status': 'error',
            'message': 'internal error'
        }), 500
    finally:
        if cursor:
            cursor.close()
