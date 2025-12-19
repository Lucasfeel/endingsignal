const ICONS = {
    webtoon: `<svg class="w-6 h-6" fill="currentColor" viewBox="0 0 24 24"><path d="M21 4H3C1.9 4 1 4.9 1 6v13c0 1.1.9 2 2 2h18c1.1 0 2-.9 2-2V6c0-1.1-.9-2-2-2zM3 19V6h8v13H3zm18 0h-8V6h8v13z"/></svg>`, // Open Book approximation
    novel: `<svg class="w-6 h-6" fill="currentColor" viewBox="0 0 24 24"><path d="M7.127 22.562l-7.127 1.438 1.438-7.128 5.689 5.69zm1.414-1.414l11.228-11.225-5.69-5.692-11.227 11.227 5.689 5.69zm9.768-21.148l-2.816 2.817 5.691 5.691 2.816-2.819-5.691-5.689z"/></svg>`, // Feather Pen approximation
    ott: `<svg class="w-6 h-6" fill="currentColor" viewBox="0 0 24 24"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-2 14.5v-9l6 4.5-6 4.5z"/></svg>`, // Play Circle
    series: `<svg class="w-6 h-6" fill="currentColor" viewBox="0 0 24 24"><path d="M21 3H3c-1.1 0-2 .9-2 2v12c0 1.1.9 2 2 2h5v2h8v-2h5c1.1 0 2-.9 2-2V5c0-1.1-.9-2-2-2zm0 14H3V5h18v12z"/></svg>`, // TV
    my: `<svg class="w-6 h-6" fill="currentColor" viewBox="0 0 24 24"><path d="M12 17.27L18.18 21l-1.64-7.03L22 9.24l-7.19-.61L12 2 9.19 8.63 2 9.24l5.46 4.73L5.82 21z"/></svg>` // Star
};

const STATE = {
    activeTab: 'webtoon',
    filters: {
        webtoon: { source: 'all', day: 'mon' },
        novel: { source: 'all', day: 'mon' },
        ott: { source: 'all', genre: 'drama' },
        series: { sort: 'latest' },
        my: { viewMode: 'subscribing' }
    },
    contents: {},
    isLoading: false,
    currentModalContent: null
};

const UI = {
    bottomNav: document.getElementById('bottomNav'),
    contentGrid: document.getElementById('contentGridContainer'),
    l1Filter: document.getElementById('l1FilterContainer'),
    l2Filter: document.getElementById('l2FilterContainer'),
    filtersWrapper: document.getElementById('filtersWrapper'),
    mySubToggle: document.getElementById('mySubToggleContainer'),
    seriesSort: document.getElementById('seriesSortOptions'),
    seriesFooter: document.getElementById('seriesFooterButton'),
    toggleIndicator: document.getElementById('toggleIndicator'),
    header: document.getElementById('mainHeader')
};

document.addEventListener('DOMContentLoaded', () => {
    renderBottomNav();
    updateTab('webtoon'); // Initial Load
    setupScrollEffect();
});

function setupScrollEffect() {
    window.addEventListener('scroll', () => {
        const scrolled = window.scrollY > 10;
        UI.filtersWrapper.style.backgroundColor = scrolled ? 'rgba(18, 18, 18, 0.85)' : '#121212';
        UI.filtersWrapper.style.backdropFilter = scrolled ? 'blur(12px)' : 'none';
        if(scrolled) UI.filtersWrapper.classList.add('border-b', 'border-white/5');
        else UI.filtersWrapper.classList.remove('border-b', 'border-white/5');
    });
}

function renderBottomNav() {
    UI.bottomNav.innerHTML = '';
    const tabs = [
        { id: 'webtoon', label: 'Webtoon', icon: ICONS.webtoon },
        { id: 'novel', label: 'Web Novel', icon: ICONS.novel },
        { id: 'ott', label: 'OTT', icon: ICONS.ott },
        { id: 'series', label: 'Series', icon: ICONS.series },
        { id: 'my', label: 'My Sub', icon: ICONS.my },
    ];

    tabs.forEach(tab => {
        const btn = document.createElement('button');
        const isActive = STATE.activeTab === tab.id;
        btn.className = `flex flex-col items-center justify-center w-full spring-bounce ${isActive ? 'text-[#4F46E5]' : 'text-[#525252]'}`;

        // Neon glow on active icon
        const iconClass = isActive ? 'scale-110 neon-drop-shadow' : 'scale-100';

        btn.innerHTML = `
            <div class="mb-1 transform transition-transform duration-200 ${iconClass}">
                ${tab.icon}
            </div>
            <span class="text-[10px] ${isActive ? 'font-bold' : 'font-medium'}">${tab.label}</span>
        `;
        btn.onclick = () => updateTab(tab.id);
        UI.bottomNav.appendChild(btn);
    });
}

async function updateTab(tabId) {
    STATE.activeTab = tabId;

    // UI Updates
    renderBottomNav();
    updateFilterVisibility(tabId);
    renderL1Filters(tabId);
    renderL2Filters(tabId);

    // Content Fetch
    await fetchAndRenderContent(tabId);

    // Restore Scroll logic would go here
    window.scrollTo({top:0});
}

function updateFilterVisibility(tabId) {
    UI.l1Filter.classList.add('hidden');
    UI.l2Filter.classList.add('hidden');
    UI.mySubToggle.classList.add('hidden');
    UI.seriesSort.classList.add('hidden');
    UI.seriesFooter.classList.add('hidden');

    if (['webtoon', 'novel', 'ott'].includes(tabId)) {
        UI.l1Filter.classList.remove('hidden');
        UI.l2Filter.classList.remove('hidden');
    } else if (tabId === 'series') {
        UI.seriesSort.classList.remove('hidden');
        UI.seriesFooter.classList.remove('hidden');
    } else if (tabId === 'my') {
        UI.mySubToggle.classList.remove('hidden');
        // Ensure grid padding handles sticky header + toggle
    }
}

function renderL1Filters(tabId) {
    UI.l1Filter.innerHTML = '';
    let items = [];

    if (tabId === 'webtoon') {
        items = [
            { id: 'all', label: '전체', color: '#A3A3A3' },
            { id: 'naver_webtoon', label: 'N', color: '#00D564' },
            { id: 'kakaowebtoon', label: 'K', color: '#F7E600' },
            { id: 'lezhin', label: 'L', color: '#E62E2E' },
            { id: 'laftel', label: 'R', color: '#6C5CE7' }
        ];
    } else if (tabId === 'novel') {
         // Series(Green), Kakao(Yellow), Ridi(Blue), Munpia
        items = [
            { id: 'all', label: 'All' },
            { id: 'naver_series', label: 'N', color: '#00D564' },
            { id: 'kakao_page', label: 'K', color: '#F7E600' },
            { id: 'ridi', label: 'R', color: '#0077D9' },
            { id: 'munpia', label: 'M' }
        ];
    } else if (tabId === 'ott') {
        items = [
            { id: 'all', label: 'All' },
            { id: 'netflix', label: 'N', color: 'red' },
            { id: 'disney', label: 'D', color: 'blue' },
            { id: 'tving', label: 'T' },
            { id: 'watcha', label: 'W' },
            { id: 'wavve', label: 'Wa' }
        ];
    }

    items.forEach(item => {
        const el = document.createElement('div');
        const isActive = STATE.filters[tabId].source === item.id;
        el.className = `l1-logo flex-shrink-0 cursor-pointer spring-bounce ${isActive ? 'active' : 'inactive'}`;
        el.textContent = item.label;
        if (item.color && isActive) el.style.borderColor = item.color;

        el.onclick = () => {
            STATE.filters[tabId].source = item.id;
            renderL1Filters(tabId); // Re-render for active state
            fetchAndRenderContent(tabId);
        };
        UI.l1Filter.appendChild(el);
    });
}

function renderL2Filters(tabId) {
    UI.l2Filter.innerHTML = '';
    let items = [];

    const days = [
        { id: 'all', label: 'ALL' },
        { id: 'mon', label: '월' },
        { id: 'tue', label: '화' },
        { id: 'wed', label: '수' },
        { id: 'thu', label: '목' },
        { id: 'fri', label: '금' },
        { id: 'sat', label: '토' },
        { id: 'sun', label: '일' },
        { id: 'daily', label: '매일' },
        { id: 'completed', label: '완결' }
    ];

    if (tabId === 'webtoon' || tabId === 'novel') {
        items = days;
    } else if (tabId === 'ott') {
        items = [
            { id: 'drama', label: '드라마' },
            { id: 'anime', label: '애니메이션' },
            { id: 'variety', label: '예능' },
            { id: 'docu', label: '다큐멘터리' }
        ];
    }

    // Determine active key
    let activeKey = '';
    if (tabId === 'webtoon' || tabId === 'novel') activeKey = STATE.filters[tabId].day || 'all'; // Default to ALL? PRD says Webtoon/Novel has ALL
    if (tabId === 'ott') activeKey = STATE.filters[tabId].genre;

    items.forEach(item => {
        const el = document.createElement('button');
        const isActive = activeKey === item.id;
        el.className = `l2-tab spring-bounce ${isActive ? 'active' : ''}`;
        el.textContent = item.label;
        el.onclick = () => {
            if (tabId === 'webtoon' || tabId === 'novel') STATE.filters[tabId].day = item.id;
            if (tabId === 'ott') STATE.filters[tabId].genre = item.id;

            renderL2Filters(tabId);
            fetchAndRenderContent(tabId);
        };
        UI.l2Filter.appendChild(el);
    });
}

function updateMySubTab(mode) {
    STATE.filters.my.viewMode = mode;
    // Animate Indicator
    UI.toggleIndicator.style.transform = mode === 'subscribing' ? 'translateX(0)' : 'translateX(100%)';
    // Text Colors logic could be added here
    fetchAndRenderContent('my');
}

async function fetchAndRenderContent(tabId) {
    UI.contentGrid.innerHTML = '';
    STATE.isLoading = true;

    // Show Skeleton
    for(i=0; i<9; i++) {
        const skel = document.createElement('div');
        skel.className = 'aspect-[3/4] rounded-lg skeleton'; // Default aspect
        UI.contentGrid.appendChild(skel);
    }

    // Mock Data or Fetch
    // For now, implementing dummy data logic aligned with existing backend capability or mocks
    let data = [];

    // Grid Aspect Ratio Class
    let aspectClass = 'aspect-[3/4]';
    if (tabId === 'novel') aspectClass = 'aspect-[1/1.4]'; // 1:1.4
    if (tabId === 'ott') aspectClass = 'aspect-[2/3]'; // 2:3

    // Need to actually fetch data here
    // Reusing existing API structure where possible
    try {
        let url = '';
        // Construct URL based on tab
        if (tabId === 'webtoon' || tabId === 'novel') {
            const day = STATE.filters[tabId].day;
            const source = STATE.filters[tabId].source;
            if (day === 'completed') {
                 url = `/api/contents/completed?type=${tabId}&source=${source}`;
            } else if (day === 'hiatus') {
                 url = `/api/contents/hiatus?type=${tabId}&source=${source}`;
            } else {
                 url = `/api/contents/ongoing?type=${tabId}&source=${source}`; // logic for days needs filter
            }
        }

        // Fetching... (Simulated delay for effect)
        await new Promise(r => setTimeout(r, 300));

        if (url) {
            const res = await fetch(url);
            const json = await res.json();

            // Client side filtering for Day if API returns grouped
            if (tabId === 'webtoon' || tabId === 'novel') {
                 const day = STATE.filters[tabId].day;
                 if (day !== 'completed' && day !== 'hiatus' && day !== 'all') {
                     data = json[day] || [];
                 } else if (day === 'all') {
                     // Aggregate all days
                     Object.values(json).forEach(arr => { if(Array.isArray(arr)) data.push(...arr); });
                 } else {
                     data = json.contents || [];
                 }
            }
        } else {
            // Mock for Series / OTT / My Sub as backend endpoints might be missing
             data = [
                { title: 'Mock Item 1', meta: { common: { thumbnail_url: null } } },
                { title: 'Mock Item 2', meta: { common: { thumbnail_url: null } } },
                { title: 'Mock Item 3', meta: { common: { thumbnail_url: null } } },
            ];
        }

    } catch (e) {
        console.error("Fetch error", e);
    }

    STATE.isLoading = false;
    UI.contentGrid.innerHTML = ''; // Clear Skeletons

    if (!data.length) {
        UI.contentGrid.innerHTML = '<div class="col-span-3 text-center text-gray-500 py-10 text-xs">콘텐츠가 없습니다.</div>';
        return;
    }

    data.forEach(item => {
        const card = createCard(item, tabId, aspectClass);
        UI.contentGrid.appendChild(card);
    });
}

function createCard(content, tabId, aspectClass) {
    const el = document.createElement('div');
    el.className = 'relative group cursor-pointer fade-in';
    const thumb = content.meta?.common?.thumbnail_url || 'https://via.placeholder.com/300x400/333/999?text=No+Img';

    // Bell Overlay for 'My Sub'
    let overlay = '';
    if (tabId === 'my') {
        const isOn = true; // Logic to check if notifications are on
        overlay = `
            <div class="absolute top-[6px] right-[6px] h-[20px] px-2 rounded-[12px] backdrop-blur-[4px] flex items-center gap-1 z-20 ${isOn ? 'bg-black/60' : 'bg-black/40'}" onclick="event.stopPropagation(); toggleBell(this)">
                <span class="text-[10px]">${isOn ? '🔔' : '🔕'}</span>
                <span class="text-[9px] font-bold ${isOn ? 'text-white' : 'text-gray-400'}">${isOn ? 'ON' : 'OFF'}</span>
            </div>
        `;
    }

    // Badge Logic (Existing)
    let badge = '';
    if (content.status === '완결') {
         badge = `
            <div class="absolute top-0 left-0 bg-black/60 backdrop-blur-md px-2 py-1 rounded-br-lg z-10 flex items-center gap-0.5">
                <span class="text-[10px] font-black text-white leading-none">EN</span>
                <span class="text-[10px] text-yellow-400 leading-none">🔔</span>
            </div>`;
    }

    // Using #1E1E1E for Card BG
    el.innerHTML = `
        <div class="${aspectClass} rounded-lg overflow-hidden bg-[#1E1E1E] relative mb-2">
            <img src="${thumb}" class="w-full h-full object-cover group-hover:scale-105 transition-transform duration-300">
            ${badge}
            ${overlay}
            <div class="absolute inset-0 bg-gradient-to-t from-black/60 via-transparent to-transparent opacity-60"></div>
        </div>
        <div class="px-0.5">
            <h3 class="font-bold text-[13px] text-[#E5E5E5] leading-[1.4] truncate">${content.title}</h3>
            <p class="text-[11px] text-[#A3A3A3] mt-0.5 truncate">${content.meta?.common?.authors?.join(', ') || ''}</p>
        </div>
    `;

    el.onclick = () => openModal(content);
    return el;
}

window.toggleBell = (el) => {
    // Dummy Toggle Logic
    const isCurrentlyOn = el.querySelector('span:last-child').innerText === 'ON';
    const newState = !isCurrentlyOn;

    if (newState) {
        el.className = el.className.replace('bg-black/40', 'bg-black/60');
        el.innerHTML = `<span class="text-[10px]">🔔</span><span class="text-[9px] font-bold text-white">ON</span>`;
    } else {
        el.className = el.className.replace('bg-black/60', 'bg-black/40');
        el.innerHTML = `<span class="text-[10px]">🔕</span><span class="text-[9px] font-bold text-gray-400">OFF</span>`;
    }
};

function openModal(content) {
    STATE.currentModalContent = content;
    document.getElementById('modalWebtoonTitle').textContent = content.title;
    document.getElementById('subscribeModal').classList.remove('hidden');
}

function closeModal() {
    document.getElementById('subscribeModal').classList.add('hidden');
    document.getElementById('emailInput').value = '';
    document.getElementById('emailError').textContent = '';
    STATE.currentModalContent = null;
}

async function submitSubscription() {
    const email = document.getElementById('emailInput').value;
    const content = STATE.currentModalContent;
    const errorMsg = document.getElementById('emailError');

    if (!email.match(/^[^\s@]+@[^\s@]+\.[^\s@]+$/)) {
        errorMsg.textContent = '올바른 이메일 형식이 아닙니다.';
        return;
    }

    try {
        const res = await fetch('/api/subscriptions', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                email: email,
                contentId: content.content_id,
                source: content.source
            })
        });
        const result = await res.json();

        if (res.ok) {
            alert('구독이 완료되었습니다!');
            closeModal();
        } else {
            errorMsg.textContent = result.message || '오류가 발생했습니다.';
        }
    } catch (e) {
        console.error(e);
        errorMsg.textContent = '서버 통신 오류';
    }
}

window.updateMySubTab = updateMySubTab;
window.closeModal = closeModal;
window.submitSubscription = submitSubscription;

