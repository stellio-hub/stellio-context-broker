(function () {
    // Wait until SphinxRtdTheme is loaded
    function disableRTDScroll() {
        if (!window.SphinxRtdTheme || !window.SphinxRtdTheme.Navigation) return;

        const nav = window.SphinxRtdTheme.Navigation;

        // Override reset function to prevent sidebar scrolling
        nav.reset = function () {
            // Do nothing
            console.log("RTD sidebar auto-scroll disabled");
        };

        // Optional: prevent scroll adjustments on window scroll/resize
        nav.onScroll = function () {};
        nav.onResize = function () {};
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", disableRTDScroll);
    } else {
        disableRTDScroll();
    }
})();
