(() => {
  const getAccessToken = () => {
    try {
      return localStorage.getItem('es_access_token');
    } catch (err) {
      return null;
    }
  };

  const showToast = (message, { type = 'info' } = {}) => {
    const container = document.getElementById('toastContainer');
    if (!container) return;

    const toast = document.createElement('div');
    toast.className = 'flex justify-center transition-opacity duration-300';

    const inner = document.createElement('div');
    const baseClasses =
      'pointer-events-auto rounded-xl border px-4 py-2 text-sm shadow-lg backdrop-blur';
    const typeClasses =
      type === 'error'
        ? 'border-red-400/40 bg-red-500/20 text-red-100'
        : type === 'success'
          ? 'border-emerald-300/40 bg-emerald-500/20 text-emerald-50'
          : 'border-white/20 bg-white/10 text-white';
    inner.className = `${baseClasses} ${typeClasses}`;
    inner.textContent = message;

    toast.appendChild(inner);
    container.appendChild(toast);

    const removeToast = () => {
      toast.classList.add('opacity-0');
      setTimeout(() => toast.remove(), 300);
    };

    setTimeout(removeToast, 2500);
  };

  const apiRequest = async (method, path, { token, body } = {}) => {
    const headers = {
      Accept: 'application/json',
    };

    if (token) {
      headers.Authorization = `Bearer ${token}`;
    }

    if (body) {
      headers['Content-Type'] = 'application/json';
    }

    const response = await fetch(path, {
      method,
      headers,
      body: body ? JSON.stringify(body) : undefined,
    });

    const contentType = response.headers.get('content-type') || '';
    let payload = null;
    if (contentType.includes('application/json')) {
      try {
        payload = await response.json();
      } catch (err) {
        payload = null;
      }
    } else {
      try {
        payload = await response.text();
      } catch (err) {
        payload = null;
      }
    }

    if (!response.ok) {
      const message =
        payload?.message ||
        payload?.error ||
        payload?.detail ||
        response.statusText ||
        '요청에 실패했습니다.';
      const error = {
        httpStatus: response.status,
        message,
      };
      if (payload?.code) error.code = payload.code;
      throw error;
    }

    return payload;
  };

  const redirectToHome = (delay = 800) => {
    setTimeout(() => {
      window.location.replace('/');
    }, delay);
  };

  const extractUser = (payload) => {
    if (!payload) return null;
    if (payload.user) return payload.user;
    if (payload.data?.user) return payload.data.user;
    return payload.data || null;
  };

  const hideGateOverlay = () => {
    const overlay = document.getElementById('adminGateOverlay');
    if (overlay) overlay.classList.add('hidden');
  };

  document.addEventListener('DOMContentLoaded', async () => {
    const token = getAccessToken();
    if (!token) {
      showToast('로그인이 필요합니다.', { type: 'error' });
      redirectToHome();
      return;
    }

    try {
      const payload = await apiRequest('GET', '/api/auth/me', { token });
      const user = extractUser(payload);

      if (user?.role !== 'admin') {
        showToast('403: Admin 권한이 필요합니다.', { type: 'error' });
        redirectToHome();
        return;
      }

      const emailEl = document.getElementById('adminUserEmail');
      if (emailEl) {
        emailEl.textContent = user?.email ? `관리자: ${user.email}` : '관리자 인증 완료';
      }

      hideGateOverlay();
    } catch (err) {
      const isForbidden = err?.httpStatus === 401 || err?.httpStatus === 403;
      const message = isForbidden
        ? '403: Admin 권한이 필요합니다.'
        : err?.message || '권한 확인 중 오류가 발생했습니다.';
      showToast(message, { type: 'error' });
      redirectToHome();
    }
  });
})();
