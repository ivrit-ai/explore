// Sets the theme before first paint, so a light-mode visitor never sees a dark flash.
// A file rather than an inline script: the Content-Security-Policy only allows
// same-origin script files. Keep in step with src/lib/theme.ts.
(function () {
  var choice = null;
  try {
    choice = localStorage.getItem('explore.theme');
  } catch (e) {
    // Storage blocked: follow the OS setting.
  }
  var light = choice === 'light' || (choice !== 'dark' && window.matchMedia('(prefers-color-scheme: light)').matches);
  document.documentElement.dataset.theme = light ? 'light' : 'dark';
  var meta = document.querySelector('meta[name="theme-color"]');
  if (meta) meta.setAttribute('content', light ? '#faf8f4' : '#121110');
})();
