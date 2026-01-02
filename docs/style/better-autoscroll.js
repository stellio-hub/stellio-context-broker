(function () {
    const sidebarSelector = ".wy-side-scroll";

    // Disable RTD theme scroll
    if (window.RTD && window.RTD.theme && typeof window.RTD.theme.scrollCurrentLinkIntoView === "function") {
        window.RTD.theme.scrollCurrentLinkIntoView = function () {};
    }

    function fixSidebarScroll(smooth = true) {
        const sidebar = document.querySelector(sidebarSelector);
        if (!sidebar) return;

        const currentLink = sidebar.querySelector(".current");
        if (!currentLink) return;

        const sidebarRect = sidebar.getBoundingClientRect();
        const linkRect = currentLink.getBoundingClientRect();

        let targetScrollTop = sidebar.scrollTop + (linkRect.top - sidebarRect.top) - sidebar.clientHeight / 2 + linkRect.height / 2;
        const maxScroll = sidebar.scrollHeight - sidebar.clientHeight;
        targetScrollTop = Math.min(Math.max(0, targetScrollTop), maxScroll);

        if (smooth) {
            sidebar.scrollTo({ top: targetScrollTop, behavior: "smooth" });
        } else {
            sidebar.scrollTop = targetScrollTop;
        }
    }

    // Initial load
    window.addEventListener("load", () => fixSidebarScroll(true));
    document.addEventListener("DOMContentLoaded", () => fixSidebarScroll(true));

    // Clicks on sidebar links (including same-page)
    document.addEventListener("click", (e) => {
        if (e.target.closest(".wy-menu-vertical a")) {
            setTimeout(() => fixSidebarScroll(true), 50);
        }
    });

    // In-page anchor links (#hash)
    window.addEventListener("hashchange", () => setTimeout(() => fixSidebarScroll(true), 50));
})();
