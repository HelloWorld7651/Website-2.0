(function () {
    const content = document.getElementById('content');
    const tocList = document.getElementById('toc-list');
    const headings = Array.from(content.querySelectorAll('h1, h2, h3'));

    const slugCounts = new Map();
    const slugify = (text) => {
        const base = text.toLowerCase()
            .trim()
            .replace(/[^a-z0-9\s-]/g, '')
            .replace(/\s+/g, '-')
            .replace(/-+/g, '-');
        const count = (slugCounts.get(base) || 0) + 1;
        slugCounts.set(base, count);
        return count > 1 ? `${base}-${count}` : base;
    };

    headings.forEach(h => {
        if (!h.id) h.id = slugify(h.textContent);
    });

    const frag = document.createDocumentFragment();
    headings.forEach(h => {
        const level = parseInt(h.tagName.substring(1), 10);
        const li = document.createElement('li');
        li.className = 'toc-item';
        const a = document.createElement('a');
        a.className = `toc-link depth-${level}`;
        a.href = `#${h.id}`;
        a.textContent = h.textContent;
        a.setAttribute('data-target', h.id);
        li.appendChild(a);
        frag.appendChild(li);
    });
    tocList.appendChild(frag);

    tocList.addEventListener('click', (e) => {
        const a = e.target.closest('a');
        if (!a) return;
        const id = a.getAttribute('data-target');
        const target = document.getElementById(id);
        if (target) {
            requestAnimationFrame(() => {
                setTimeout(() => target.setAttribute('tabindex', '-1'), 0);
                setTimeout(() => target.focus({preventScroll:true}), 350);
            });
        }
    });

    const linksById = new Map(
        Array.from(tocList.querySelectorAll('a')).map(a => [a.getAttribute('data-target'), a])
    );

    const markActive = (id) => {
        linksById.forEach((a, key) => a.setAttribute('aria-current', key === id ? 'true' : 'false'));
    };

    const io = new IntersectionObserver((entries) => {
        let topMost = entries
            .filter(e => e.isIntersecting)
            .sort((a,b) => a.boundingClientRect.top - b.boundingClientRect.top)[0];
        if (topMost) markActive(topMost.target.id);
    }, { rootMargin: '-20% 0px -70% 0px', threshold: [0, 0.25, 0.5, 1] });

    headings.forEach(h => io.observe(h));
})();
