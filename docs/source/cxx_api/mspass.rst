.. _mspass_namespace:

MsPASS C++ API
==============

.. raw:: html

    <style>
    .mspass-doxygen-frame {
        background: var(--pst-color-background);
        border: 1px solid var(--pst-color-border);
        border-radius: 6px;
        box-sizing: border-box;
        height: calc(100vh - 13rem);
        max-width: 100%;
        min-height: 560px;
        width: 100%;
    }

    @media screen and (max-width: 768px) {
        .mspass-doxygen-frame {
            height: 70vh;
            min-height: 420px;
        }
    }
    </style>

    <script>
    (function () {
        const defaultPage = "hierarchy.html";
        const doxygenRoot = "../_static/html/";
        const statePrefix = "doxygen=";

        function normalizeDoxygenPath(value) {
            if (!value) {
                return defaultPage;
            }

            try {
                value = decodeURIComponent(value);
            } catch (error) {
                return defaultPage;
            }

            value = value
                .replace(/^(\.\.\/)?_static\/html\//, "")
                .replace(/^html\//, "");

            try {
                const parsed = new URL(value, "https://mspass.invalid/");
                const path = parsed.pathname.replace(/^\//, "");
                if (
                    parsed.origin !== "https://mspass.invalid" ||
                    path.includes("..") ||
                    !path.endsWith(".html")
                ) {
                    return defaultPage;
                }
                return path + parsed.search + parsed.hash;
            } catch (error) {
                return defaultPage;
            }
        }

        function doxygenPathFromHash() {
            const hash = window.location.hash.replace(/^#/, "");
            if (!hash.startsWith(statePrefix)) {
                return null;
            }
            return normalizeDoxygenPath(hash.slice(statePrefix.length));
        }

        function requestedDoxygenPath() {
            const hashPath = doxygenPathFromHash();
            if (hashPath) {
                return hashPath;
            }

            const params = new URLSearchParams(window.location.search);
            return normalizeDoxygenPath(params.get("doxygen"));
        }

        function setDoxygenState(doxygenPath) {
            const normalizedPath = normalizeDoxygenPath(doxygenPath);
            const url = new URL(window.location.href);
            url.searchParams.delete("doxygen");
            url.hash = normalizedPath === defaultPage
                ? ""
                : statePrefix + encodeURIComponent(normalizedPath);

            if (url.href === window.location.href) {
                return;
            }

            try {
                window.history.replaceState(null, "", url.href);
            } catch (error) {
                if (url.hash) {
                    window.location.replace(url.hash);
                }
            }
        }

        function setFramePath(frame, doxygenPath) {
            const normalizedPath = normalizeDoxygenPath(doxygenPath);
            if (frame.dataset.doxygenPath === normalizedPath) {
                return;
            }
            frame.dataset.doxygenPath = normalizedPath;
            frame.src = doxygenRoot + normalizedPath;
        }

        function syncUrlFromFrame(frame) {
            let frameUrl;
            try {
                frameUrl = new URL(frame.contentWindow.location.href);
            } catch (error) {
                return;
            }

            const rootUrl = new URL(doxygenRoot, window.location.href).href;
            if (!frameUrl.href.startsWith(rootUrl)) {
                return;
            }

            const doxygenPath = normalizeDoxygenPath(
                frameUrl.href.slice(rootUrl.length)
            );
            frame.dataset.doxygenPath = doxygenPath;
            setDoxygenState(doxygenPath);
        }

        function selectedTheme() {
            const theme = document.documentElement.getAttribute("data-theme");
            if (theme === "dark" || theme === "light") {
                return theme;
            }
            if (window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches) {
                return "dark";
            }
            return "light";
        }

        function syncThemeToFrame(frame) {
            try {
                frame.contentWindow.postMessage(
                    { type: "mspass-doc-theme", theme: selectedTheme() },
                    "*"
                );
            } catch (error) {
                return;
            }
        }

        function resizeDoxygenFrame(frame) {
            const compactLayout = window.matchMedia("(max-width: 768px)").matches;
            const minHeight = compactLayout ? 420 : 560;
            const availableHeight = window.innerHeight
                - frame.getBoundingClientRect().top;
            frame.style.height = Math.max(minHeight, availableHeight) + "px";
        }

        window.addEventListener("DOMContentLoaded", function () {
            const frame = document.getElementById("mspass-doxygen-frame");
            if (!frame) {
                return;
            }

            resizeDoxygenFrame(frame);
            setFramePath(frame, requestedDoxygenPath());

            window.addEventListener("resize", function () {
                resizeDoxygenFrame(frame);
            });

            const themeObserver = new MutationObserver(function () {
                syncThemeToFrame(frame);
            });
            themeObserver.observe(document.documentElement, {
                attributes: true,
                attributeFilter: ["data-theme"],
            });

            if (window.matchMedia) {
                const colorSchemeQuery = window.matchMedia("(prefers-color-scheme: dark)");
                if (colorSchemeQuery.addEventListener) {
                    colorSchemeQuery.addEventListener("change", function () {
                        syncThemeToFrame(frame);
                    });
                } else if (colorSchemeQuery.addListener) {
                    colorSchemeQuery.addListener(function () {
                        syncThemeToFrame(frame);
                    });
                }
            }

            frame.addEventListener("load", function () {
                resizeDoxygenFrame(frame);
                syncThemeToFrame(frame);
                syncUrlFromFrame(frame);
                try {
                    frame.contentWindow.addEventListener(
                        "hashchange",
                        function () {
                            syncUrlFromFrame(frame);
                        }
                    );
                } catch (error) {
                    return;
                }
            });

            window.addEventListener("message", function (event) {
                if (
                    !event.data ||
                    event.data.type !== "mspass-doxygen-location" ||
                    !event.data.path
                ) {
                    return;
                }

                const doxygenPath = normalizeDoxygenPath(event.data.path);
                frame.dataset.doxygenPath = doxygenPath;
                setDoxygenState(doxygenPath);
            });

            window.addEventListener("hashchange", function () {
                setFramePath(frame, requestedDoxygenPath());
            });

            window.addEventListener("popstate", function () {
                setFramePath(frame, requestedDoxygenPath());
            });
        });
    })();
    </script>

    <iframe
        id="mspass-doxygen-frame"
        class="mspass-doxygen-frame"
        src="../_static/html/hierarchy.html"
        title="MsPASS C++ API Doxygen hierarchy"
        scrolling="yes">
    </iframe>

.. .. doxygennamespace:: mspass
..    :members:
